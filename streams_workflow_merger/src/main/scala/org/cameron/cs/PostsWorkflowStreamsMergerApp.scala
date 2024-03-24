package org.cameron.cs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.cameron.cs.StreamsWorkflowMergerConfig.parser
import scopt.OParser

object PostsWorkflowStreamsMergerApp extends App {

  val conf: StreamsWorkflowMergerConfig =
    OParser.parse(parser, args, StreamsWorkflowMergerConfig()) match {
      case Some(config) => config
      case _ => throw new RuntimeException("Missing mandatory program params")
    }

  val sparkConf: SparkConf = new SparkConf

  implicit val spark: SparkSession =
    SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate

  val processor = new StreamsWorkflowMergerProcessor(spark, conf)

  processor.processMergePosts()
}
