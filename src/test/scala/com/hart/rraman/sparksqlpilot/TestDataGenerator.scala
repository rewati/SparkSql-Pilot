
package com.hart.rraman.sparksqlpilot

import java.io.{File, PrintWriter}

import scala.util.Random

/**
  * Created by Rewati Raman (rewati.raman@hart.com).
  */
object TestDataGenerator extends App{
  val pw = new PrintWriter(new File("src/test/resources/data2.json" ))
  val empNameList = Array("Rewati Raman","Chris Barron","Ryan Trans",
    "Hoa Ho", "A Raman", "Test1"," Test2 ","Test3","Test4")
  val depList = Array("It","Data","Accounts","Transport","Dep1","Kitchen","Area51","Ui","Secret Dep")
  val rnd = new Random()

  def getRandom(ar: Array[String]) : String = {
    ar(rnd.nextInt(ar.length))
  }

  def createEmployee(i: Int): String = {
    val jsonString = """{"id":"""+i+""","name":"""" + getRandom(empNameList) + """", "age":""" + rnd.nextInt(60) + ""","department":"""" + getRandom(depList) + """"}"""
    jsonString
  }
  for(i <- 1000 to 900000) pw.write(createEmployee(i) + "\n")

  pw.close

  val pw1 = new PrintWriter(new File("src/test/resources/data3.json" ))

  depList.foreach(x => {
    pw1.write("""{"department":""""+x+"""","description":""""+x+" is a test description"+""""}"""+"\n")
  })
  pw1.close
}
