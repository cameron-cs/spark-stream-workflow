package org.cameron.cs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import MetricsWorkflowStreamConfig._
import scopt.OParser

object MetricsWorkflowStreamApp extends App {

  val conf: MetricsWorkflowStreamConfig =
    OParser.parse(parser, args, MetricsWorkflowStreamConfig()) match {
      case Some(config) => config
      case _            => throw new RuntimeException("Missing mandatory program params")
    }

  val sparkConf: SparkConf = new SparkConf

  implicit val spark: SparkSession =
    SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate

  val processor = new MetricsWorkflowStreamProcessor(spark, conf)

  processor.process()
}
