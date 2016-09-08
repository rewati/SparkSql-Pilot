
package com.hart.rraman.sparksqlpilot

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

/**
  * Created by Rewati Raman (rewati.raman@hart.com).
  */
class JsonFileToSQLRDDProcessor$Test extends FunSuite {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL from Json file")
    .config("spark.some.config.option", "some-value").master("local[*]")
    .getOrCreate()

  val jsonDataFile = "src/test/resources/data1.json"
  val jsonSqlDataframe = new JsonFileToSQLRDDProcessor(spark)
  val dataFrame = jsonSqlDataframe.getDataFrameFromJsonFile(jsonDataFile)

  test("Read from Json file and print as table") {
    dataFrame.show()
    assert(dataFrame.count() === 9)
  }

  test("Filter by name") {
    val result = jsonSqlDataframe.filterByName(dataFrame,"Andy")
    val resultreduced = result.collect()
    resultreduced.foreach(x => println(x))
    assert(resultreduced.size === 1)
  }

  test("Group by age") {
    val result = jsonSqlDataframe.groupByCoulmn(dataFrame,"age")
    result.count().show()
    assert(result.count().count() === 5)
  }





}
