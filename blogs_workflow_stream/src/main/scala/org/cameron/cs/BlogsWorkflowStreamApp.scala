package org.cameron.cs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import BlogsWorkflowStreamConfig._
import scopt.OParser

object BlogsWorkflowStreamApp extends App {

  val conf: BlogsWorkflowStreamConfig =
    OParser.parse(parser, args, BlogsWorkflowStreamConfig()) match {
      case Some(config) => config
      case _            => throw new RuntimeException("Missing mandatory program params")
    }

  val sparkConf: SparkConf = new SparkConf

  implicit val spark: SparkSession =
    SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate

  val processor = new BlogsWorkflowStreamProcessor(spark, conf)

  processor.process()
}
