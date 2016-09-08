
package com.hart.rraman.sparksqlpilot

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Rewati Raman (rewati.raman@hart.com).
  */
object Application extends App{
  val conf = new SparkConf().setAppName("Simple Application")
  conf.setMaster("local[*]")
  val sc = new SparkContext(conf)
  val someFile = "src/main/resources/data1.txt"
  val data = sc.textFile(someFile, 2).cache()
  val count = data.filter(line => line.contains("a")).count()
  println(count)

}
