package com.sparkrareexamples.executionMetrics

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.sparkrareexamples.executionMetrics.ExecutionUtility.{actionHelperFun, registerListener, sqlHelperFun}

object ExecutionMetrics {

  def main(args: Array[String]): Unit = {
    println("Hello simpleApp")
    implicit val spark = SparkSession.builder.master("local[*]").appName("First spark program").getOrCreate()
    registerListener
    val data = spark.read.option("header","true").csv(getClass.getResource("/person_data.csv").getPath)
    val x = actionHelperFun(data.show)
    val y = actionHelperFun(data.write.mode(SaveMode.Overwrite).csv, System.getProperty("user.dir")+"/target/data/output")
    println(s"Metrics : $x to execute show")
    println(s"Metrics : $y to execute write")

    //writing using sqls
    data.createOrReplaceTempView("temp_tbl")

    val ddl =
      """create table IF NOT EXISTS default.employee_tbl
        |(
        |id string,
        |first_name string,
        |last_name string,
        |email string,
        |gender string,
        |country string
        |) using parquet""".stripMargin

    val insertSql =
      """
        |insert overwrite table default.employee_tbl
        |select * from temp_tbl
        |""".stripMargin

    spark.sql("DROP TABLE IF EXISTS default.employee_tbl")

    val ddlMetrics = sqlHelperFun(ddl)
    val insertMetrics = sqlHelperFun(insertSql)

    println(ddlMetrics)
    println(insertMetrics)
    spark.table("default.employee_tbl").show
  }
}
