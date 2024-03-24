package org.cameron.cs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.cameron.cs.conf.DataCleanerConfig
import org.cameron.cs.conf.DataCleanerConfig.parser
import scopt.OParser

object BlogsCleanerApp extends App {

  val conf: DataCleanerConfig =
    OParser.parse(parser, args, DataCleanerConfig()) match {
      case Some(config) => config
      case _            => throw new RuntimeException("Missing mandatory program params")
    }

  val sparkConf: SparkConf = new SparkConf

  implicit val spark: SparkSession =
    SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate

  val processor = new DataCleanerProcessor(spark)

  processor.cleanData(conf.lowerBound, conf.upperBound, conf.hdfsDataPath, conf.skipTrash.toLowerCase.toBoolean)(conf.cleanerDataName)
}
