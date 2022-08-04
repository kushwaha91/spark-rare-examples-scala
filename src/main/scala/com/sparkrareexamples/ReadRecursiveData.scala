package com.sparkrareexamples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.UDTRegistration
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object ReadRecursiveData {
  implicit val spark = SparkSession.builder.master("local[*]").appName("Recursive Data Read").getOrCreate
  import spark.implicits._
  UDTRegistration.register(classOf[ElementRoot].getName, classOf[ElementRecord].getName)

  def readJsonDataToDS(source: String) = {
    implicit val formats = DefaultFormats
    spark.createDataset(parse(scala.io.Source.fromURL(getClass.getResource(source)).mkString).extract[Seq[ElementNode]])
  }

  def readJsonDataToDF(source: String) = {
    implicit val formats = DefaultFormats
    spark.createDataFrame(parse(scala.io.Source.fromURL(getClass.getResource(source)).mkString).extract[Seq[ElementNode]])
  }


  def main(args: Array[String]): Unit = {

    val dataDS = readJsonDataToDS("/recursive_data.json")
    dataDS.show(100,false)

    val dataDF = readJsonDataToDF("/recursive_data.json")
    dataDF.show(100,false)






  }

}
