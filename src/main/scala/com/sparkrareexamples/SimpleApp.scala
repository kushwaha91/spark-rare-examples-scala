package com.sparkrareexamples

import org.apache.spark.sql.SparkSession


object SimpleApp{
  def main(args: Array[String]): Unit ={
    println("Hello simpleApp")
    val spark = SparkSession.builder.master("local[*]").appName("First spark program").getOrCreate()
    spark.sql("select 'Hello from Spark!' as message").show(1000,false)

  }
}