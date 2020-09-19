package net.martinprobson.spark

import java.io.InputStream

import org.apache.spark.{Logging, SparkConf, SparkContext}
//import org.scalatest.{Outcome, fixture}

class SparkTest {
  //extends fixture.FunSuite with Logging {



/*  def withFixture(test: OneArgTest): Outcome = {
    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("local[*]")

    val spark =  new SparkContext(conf)
    try {
      withFixture(test.toNoArgTest(spark))
    } finally spark.stop
  }

  test("empsRDD rowcount") { spark =>
    val empsRDD = spark.parallelize(getInputData("/data/employees.json"), 5)
    assert(empsRDD.count === 1000)
  }

  test("titlesRDD rowcount") { spark =>
    val titlesRDD = spark.parallelize(getInputData("/data/titles.json"), 5)
    assert(titlesRDD.count === 1470)
  }*/

  private def getInputData(name: String): Seq[String] = {
    val is: InputStream = getClass.getResourceAsStream(name)
    scala.io.Source.fromInputStream(is).getLines.toSeq
  }
}