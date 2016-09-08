
package com.hart.rraman.sparksqlpilot

import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

/**
  * Created by Rewati Raman (rewati.raman@hart.com).
  */
class JsonFileToSQLRDDProcessor(spark: SparkSession) {

  def getDataFrameFromJsonFile(fileString: String): DataFrame = {
    spark.read.json(fileString)
  }

  /**
    * Filter by name
    * @param dataFrame
    * @param name
    * @return RDD[Row]
    */
  def filterByName(dataFrame: DataFrame, name: String): DataFrame = {
    dataFrame.filter(x => x(2) == name )
  }

  /**
    * Filter by column
    * @param dataFrame
    * @param columnName
    * @param value
    * @return
    */
  def filterByColumn(dataFrame: DataFrame,columnName: String, value: String): DataFrame = {
    dataFrame.filter(col(columnName) === value )
  }

  /**
    * Group by column
    * @param dataFrame
    * @param columnName
    * @return
    */
  def groupByCoulmn(dataFrame: DataFrame,columnName: String): RelationalGroupedDataset = {
    dataFrame.groupBy(columnName)
  }
}
