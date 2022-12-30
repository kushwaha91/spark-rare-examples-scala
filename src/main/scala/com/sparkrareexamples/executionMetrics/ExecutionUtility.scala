package com.sparkrareexamples.executionMetrics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.util.QueryExecutionListener

object ExecutionUtility {

  lazy val queryListener = new QueryExecutionListener {
    var metric : Map[String, SQLMetric] = Map()
    var duration: Long = 0L

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long) = {
      //println(funcName)
      metric = qe.executedPlan.metrics
      duration = durationNs/1000000
    }

    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception) = ()
  }

  def actionHelperFun(f:() => Unit): Metrics = {
    f.apply
    Metrics(
      queryListener.metric.get("numFiles").map(_.value).getOrElse(0),
      queryListener.metric.get("numOutputRows").map(_.value).getOrElse(0),
      queryListener.metric.get("numParts").map(_.value).getOrElse(0),
      queryListener.metric.get("numOutputBytes").map(_.value).getOrElse(0),
      queryListener.duration)
  }

  def actionHelperFun(f: String => Unit, path: String): Metrics = {
    f.apply(path)
    Metrics(
      queryListener.metric.get("numFiles").map(_.value).getOrElse(0),
      queryListener.metric.get("numOutputRows").map(_.value).getOrElse(0),
      queryListener.metric.get("numParts").map(_.value).getOrElse(0),
      queryListener.metric.get("numOutputBytes").map(_.value).getOrElse(0),
      queryListener.duration)
  }


  def sqlHelperFun(sqlString: String)(implicit spark:SparkSession ): Metrics = {
    val qe = spark.sql(sqlString).queryExecution.executedPlan.metrics
    Metrics(
      queryListener.metric.get("numFiles").map(_.value).getOrElse(0),
      queryListener.metric.get("numOutputRows").map(_.value).getOrElse(0),
      queryListener.metric.get("numParts").map(_.value).getOrElse(0),
      queryListener.metric.get("numOutputBytes").map(_.value).getOrElse(0),
      queryListener.duration)
  }

  def registerListener()(implicit spark: SparkSession) = {
    spark.sqlContext.listenerManager.register(queryListener)

  }

}
