package net.martinprobson.spark

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{EmptyRow, GenericRowWithSchema}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead, udf}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.{asJavaCollection, asScalaIterator}
import scala.collection.JavaConverters.{asScalaBufferConverter, mutableSeqAsJavaListConverter}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


case class DailyStatus(functionId: String, day: String, status: String, nextGreen: Timestamp)

object GroupByApp {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("local[*]")

    val spark = new SparkContext(conf)
    import spark._
    val hiveContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(spark);
    import org.apache.spark.sql.functions._
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


    val seqRow = Seq(
      (Timestamp.valueOf("2019-07-01 12:01:19.000"), "FAIL", "FUNC1"),
      (Timestamp.valueOf("2019-07-01 12:02:19.000"), "FAIL", "FUNC_CLOSE_IMMEDIATELY"),
      (Timestamp.valueOf("2019-07-01 12:02:20.000"), "PASS", "FUNC_CLOSE_IMMEDIATELY"),
      (Timestamp.valueOf("2019-07-02 12:01:19.000"), "FAIL", "FUNC1"),
      (Timestamp.valueOf("2019-07-03 12:01:19.000"), "FAIL", "FUNC1"),
      (Timestamp.valueOf("2019-07-04 13:01:19.000"), "PASS", "FUNC1")

    )


    val rdd = spark.parallelize(seqRow)


    hiveContext.sql("DROP TABLE IF EXISTS producer_messages")

    hiveContext.sql(
      """
        CREATE EXTERNAL TABLE IF NOT EXISTS producer_messages (execution_time timestamp, test_result string, function_id string) PARTITIONED BY (execution_day string) STORED AS PARQUET
        |""".stripMargin)

    val input_df: DataFrame = hiveContext.createDataFrame(rdd)
      .toDF("execution_Time", "test_result", "function_id")
      .withColumn("execution_day", truncateTimestampConvertToDateUDF(col("execution_time")))

    input_df.registerTempTable("temp_view")
    hiveContext.sql("insert overwrite table producer_messages partition (execution_day) select * from temp_view")

    // hiveContext.sql("select * from producer_messages").show()
    val sparkSqlResultsFromProducerMessages = hiveContext.sql("select function_id, execution_day, count(*) cnt, collect_list(struct(execution_time, test_result)) executions  " +
      "from producer_messages group by function_id, execution_day order by function_id, execution_day asc")

    val aggregates = sparkSqlResultsFromProducerMessages.rdd.groupBy(f => f.getString(0)).foldByKey(new mutable.MutableList[Row])((accumList, byDayFunctionAggregate) => {
      val res = byDayFunctionAggregate.foldLeft(
        (new ListBuffer[Tuple4[String, String, String, Timestamp]], new ListBuffer[Tuple4[String, String, String, Timestamp]])
      )((accum, element) => {
        val (func, day) = (element.getString(0), element.getString(1))
        val status: mutable.Buffer[GenericRowWithSchema] = element.getList(3).asScala;

        val mapped = status.view.map(p => Tuple2.apply(p.getString(1), p.getTimestamp(0)));

        if (accum._2.size > 0) {
          //look for a pass the following day
          val pass = mapped.find(p => p._1.equals("PASS")).headOption
          if (pass.isDefined) {
            val closedStatuses = accum._2.map(tpl => tpl.copy(_4 = pass.get._2))
            logger.info("Closing statues : " + closedStatuses.size)
            closedStatuses.foreach(closed => accum._1 += closed)
            accum._2.clear()
          } else {
            logger.info("No pass today, will try next day")
          }

        }
        val failed = mapped find (p => p._1.equals("FAIL")) headOption
        val pass = if (failed.isDefined) mapped find (p => p._1.equals("PASS") && p._2.after(failed.get._2)) headOption else None
        val statusDaily = if (failed.isDefined) "FAIL" else "PASS"
        val nextGreen = if (pass.isDefined) pass.get._2 else null;
        val dailyResult = Tuple4(func, day, statusDaily, nextGreen)

        //add to the list to be checked later
        if (failed.isDefined && dailyResult._4 == null) {
          //make sure we consider only the first failed
          if (accum._2.size == 0)
            accum._2 += dailyResult
        } else {
          accum._1 += dailyResult
        }
        accum
      })
      //merge existing results with the new results
      accumList.toList ::: res._1.map(f => Row.fromTuple(f)).toList
    })


    val list: RDD[Row] =aggregates.flatMap(_._2.toList.map(o=>o))
    def dfSchema(): StructType =
      StructType(
        Seq(
          StructField(name = "function_id", dataType = StringType, nullable = false),
          StructField(name = "day", dataType = StringType, nullable = false),
          StructField(name = "status", dataType = StringType, nullable = false),
          StructField(name = "next_green", dataType = TimestampType, nullable = true)
        )
      )

   val dataSet =  hiveContext.createDataFrame(list, dfSchema())

    dataSet.show(false)
  }

  def convertStringToDate(ts: Timestamp) = {
    val res = new SimpleDateFormat("dd-MM-yyyy").format(ts)
    new Timestamp(new SimpleDateFormat("dd-MM-yyyy").parse(res).getTime)
  }


  val truncateTimestampConvertToDateUDF = udf(convertStringToDate _)
}

