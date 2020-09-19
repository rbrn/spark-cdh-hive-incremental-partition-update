package net.martinprobson.spark

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}



object SimpleApp {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("local[*]")

    val spark =  new SparkContext(conf)


    val df = spark.parallelize(List(1, 2, 3))

    val hiveContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(spark);
    import  hiveContext._

    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    val seqRow = Seq(
      (Timestamp.valueOf("2019-07-01 12:01:19.000"), "PASS", "FUNC1")
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


    hiveContext.sql("select * from producer_messages").show()

    val updateSeq = Seq(
      (Timestamp.valueOf("2019-07-11 12:01:19.000"), "PASS", "FUNC1")
    )
    val rddUpdate  = spark.parallelize(updateSeq)
    val update : DataFrame  = hiveContext.createDataFrame(rddUpdate)
      .toDF("execution_time", "test_result", "function_id")
      .withColumn("execution_day", truncateTimestampConvertToDateUDF(col("execution_time")))


    update.registerTempTable("temp_view2")
    hiveContext.sql("insert overwrite table producer_messages partition (execution_day) select * from temp_view2")

    hiveContext.sql("select * from producer_messages").show()

    val updateSeq2 = Seq(
      (Timestamp.valueOf("2019-07-12 12:01:19.000"), "PASS", "FUNC1")
    )
    val rddUpdate2  = spark.parallelize(updateSeq2)
    val update2 : DataFrame  = hiveContext.createDataFrame(rddUpdate2)
      .toDF("execution_time", "test_result", "function_id")
      .withColumn("execution_day", truncateTimestampConvertToDateUDF(col("execution_time")))


    update2.registerTempTable("temp_view3")
    hiveContext.sql("insert overwrite table producer_messages partition (execution_day) select * from temp_view3")

    hiveContext.sql("select * from producer_messages").show()

  }
  val dateFormat = new SimpleDateFormat("dd-MM-yyyy")
  def convertStringToDate(ts: Timestamp) = {
    dateFormat.format(ts)
  }



  val truncateTimestampConvertToDateUDF = udf(convertStringToDate _)
  }

