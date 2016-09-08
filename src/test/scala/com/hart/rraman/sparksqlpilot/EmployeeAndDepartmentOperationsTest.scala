
package com.hart.rraman.sparksqlpilot

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

/**
  * Created by Rewati Raman (rewati.raman@hart.com).
  */
class EmployeeAndDepartmentOperationsTest extends FunSuite {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL from Json file")
    .config("spark.some.config.option", "some-value").master("local[*]")
    .getOrCreate()

  val employeeDataFile = "src/test/resources/data2.json"
  val jsonSqlDataframe = new JsonFileToSQLRDDProcessor(spark)
  val dataFrame = jsonSqlDataframe.getDataFrameFromJsonFile(employeeDataFile)
  val sql1 = "select employee.name as name,employee.age as age,employee.department " +
    "as department, d.description as description from employee  JOIN department as " +
    "d on employee.department =  d.department where employee.name='Rewati Raman' and " +
    "age < 50 and age > 40 "

  test("Read from Json file and do various funny stuff :) using spark dataFrame") {
    println(" ***************** Print 50 rows ***********************")

    dataFrame.show(50)
    println(" ***************** Print 50 rows whose name are REWATI RAMAN " +
      "and age between 50 to 55 and work in department area 51 or Secret Dep" +
      "***********************")

    dataFrame.filter(col("name") === "Rewati Raman" )
      .filter(col("age") > 50).filter(col("age") < 55)
      .filter(col("department") === "Area51" || col("department") === "Secret Dep")
      .show(50)

    println("*************Print row count whose name are REWATI RAMAN " +
      "and age between 50 to 55 and work in department area 51 or Secret Dep ")

    val c = dataFrame.filter(col("name") === "Rewati Raman" )
      .filter(col("age") > 50).filter(col("age") < 55)
      .filter(col("department") === "Area51" || col("department") === "Secret Dep")
      .count()
    println(c)

    println("******** group by department *********")

    dataFrame.filter(col("name") === "Rewati Raman" )
      .filter(col("age") > 50).filter(col("age") < 55)
      .filter(col("department") === "Area51" || col("department") === "Secret Dep")
      .groupBy("department").count().show();
    dataFrame.createTempView("employee")

    val departmentDataFile2 = "src/test/resources/data3.json"
    val dataFrame1 = jsonSqlDataframe.getDataFrameFromJsonFile(departmentDataFile2)
    dataFrame1.createTempView("department")

    println("***************** select * from employee ************")

    spark.sql("select * from employee").show(100)
    spark.sql(sql1).show(50)
    dataFrame.filter(col("name") === "Rewati Raman" )
      .filter(col("age") > 0).filter(col("age") < 55)
      .filter(col("department") === "Area51" || col("department") === "Secret Dep").createOrReplaceTempView("employee1")
    spark.sql(sql1).show(50)

    dataFrame1.write.parquet("src/test/resources/data3.parquet")
    val parquetFileDF = spark.read.parquet("src/test/resources/data3.parquet")
    parquetFileDF.filter(col("department") === "Secret Dep").show()
  }
}
