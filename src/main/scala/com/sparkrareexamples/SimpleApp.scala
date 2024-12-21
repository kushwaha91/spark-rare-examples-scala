package com.sparkrareexamples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Calendar


object SimpleApp {
  def main(args: Array[String]): Unit = {

    val windowSpec  = Window.partitionBy("serial","desc").orderBy(col("ingestion").desc)
    println("Hello simpleApp")

    val spark = SparkSession.builder.master("local[*]").appName("First spark program").getOrCreate()
    spark.sql("select 'Hello from Spark!' as message").show(1000,false)

    val currdate: String =
      LocalDate.now.format(DateTimeFormatter.ofPattern("yyyyMMdd"))


    println(currdate)
    println(Calendar.getInstance().getTime)

  }
}