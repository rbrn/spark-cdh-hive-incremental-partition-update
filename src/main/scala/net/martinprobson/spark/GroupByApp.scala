package net.martinprobson.spark

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{EmptyRow, GenericRowWithSchema}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead, udf}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions.asScalaIterator
import scala.collection.mutable


object GroupByApp {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("local[*]")

    val spark =  new SparkContext(conf)

    val hiveContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(spark);
    import org.apache.spark.sql.functions._
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


    val seqRow = Seq(
      (Timestamp.valueOf("2019-07-01 12:01:19.000"), "FAIL", "FUNC1"),
      (Timestamp.valueOf("2019-07-01 13:01:19.000"), "PASS", "FUNC1"),
      (Timestamp.valueOf("2019-07-02 12:01:19.000"), "FAIL", "FUNC1"),
      (Timestamp.valueOf("2019-07-02 14:01:19.000"), "PASS", "FUNC2"),
        (Timestamp.valueOf("2019-07-02 12:01:19.000"), "FAIL", "FUNC1"),
      (Timestamp.valueOf("2019-07-02 13:01:19.000"), "PASS", "FUNC1"),
        (Timestamp.valueOf("2019-07-03 312:01:19.000"), "FAIL", "FUNC1"),
      (Timestamp.valueOf("2019-07-03 13:01:19.000"), "PASS", "FUNC1")
    )



    val rdd  = spark.parallelize(seqRow)


    hiveContext.sql("DROP TABLE IF EXISTS producer_messages")

    hiveContext.sql(
      """
        CREATE EXTERNAL TABLE IF NOT EXISTS producer_messages (execution_time timestamp, test_result string, function_id string) PARTITIONED BY (execution_day string) STORED AS PARQUET
        |""".stripMargin)

    val input_df : DataFrame  = hiveContext.createDataFrame(rdd)
      .toDF("execution_Time", "test_result", "function_id")
      .withColumn("execution_day", truncateTimestampConvertToDateUDF(col("execution_time")))

    input_df.registerTempTable("temp_view")
    hiveContext.sql("insert overwrite table producer_messages partition (execution_day) select * from temp_view")

   // hiveContext.sql("select * from producer_messages").show()
    case class DailyStatus( functionId: String, day: String, status: String, nextGreen: String )
    val res = hiveContext.sql("select function_id, execution_day, count(*) cnt, collect_list(struct(execution_time, test_result)) executions  " +
      "from producer_messages group by function_id, execution_day order by function_id, execution_day asc")

      res.rdd.groupBy(f=> f.getString(0)).foldByKey(new mutable.MutableList[Row])((accumList, byDayFunctionAggregate) => {
        print(accumList)

        val res = byDayFunctionAggregate.foldLeft( new mutable.MutableList[DailyStatus]) ((accum, element) => {
          println(accum)

          val func = element.getString(0)
          val day = element.getString(1)
          /*
          Iterate in list and find if it's failed.
          It it's failed, look for a green this day.
          If no green is today, look for one next day
           */
          val status = element.getList(3).stream()

          val accn= accum  += DailyStatus(functionId = func, day = day, status = "???", nextGreen = day)
          accn
        })
        accumList
      }).foreach(println)


    /* val res = hiveContext.sql("select function_id, execution_day, count(*) cnt, collect_list(struct(execution_time, test_result)) executions  " +
       "from producer_messages group by function_id, execution_day order by execution_day")

     res. registerTempTable("execution_by_day")

     val rawMessages = hiveContext.sql("select * from execution_by_day").show(false)
 */

  /*  hiveContext.udf.register("findNextGreenMessage", (data: Timestamp, list: mutable.WrappedArray[GenericRowWithSchema]) => {
      val result = list.find( f=> f.getTimestamp(0).after(data) && f.getString(1).equals("PASS")).map( o=> o.getTimestamp(0))
      result.orElse(null)
    } )

    hiveContext.sql(" select *, findNextGreenMessage(m.execution_Time, e.executions) next_green from  producer_messages  m " +
      "join execution_by_day e on e.function_id = m.function_id and e.execution_day = m.execution_day where m.test_result = 'FAIL' ")
      .show(false)*/

  }

  def convertStringToDate(ts: Timestamp) = {
   val res = new SimpleDateFormat("dd-MM-yyyy").format(ts)
    new Timestamp(new SimpleDateFormat("dd-MM-yyyy"). parse(res).getTime)
  }


  val truncateTimestampConvertToDateUDF = udf(convertStringToDate _)
  }

