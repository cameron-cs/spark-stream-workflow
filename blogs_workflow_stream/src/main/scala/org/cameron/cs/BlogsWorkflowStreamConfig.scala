package org.cameron.cs

import scopt.{OParser, OParserBuilder}

/**
 * Configuration class for BlogsStreamProcessor.
 * This class encapsulates all necessary configurations required by the BlogsStreamProcessor to read from Kafka and write to HDFS.
 *
 * @param execDate The execution date for the current processing job.
 * @param kafkaHost Kafka host information.
 * @param kafkaConsumerGroup Kafka consumer group ID.
 * @param blogsTopicName Name of the Kafka topic to read blogs data from.
 * @param hdfsPath Path in HDFS where the processed data should be written.
 * @param hdfsOffsetsPath Path in HDFS to store and read offsets.
 */
case class BlogsWorkflowStreamConfig(execDate: String                = "",
                                     kafkaHost: String               = "",
                                     kafkaConsumerGroup: String      = "",
                                     blogsTopicName: String          = "",
                                     hdfsPath: String                = "",
                                     hdfsOffsetsPath: String         = "")

/**
 * Companion object for BlogsStreamConfig case class.
 * Contains the command-line argument parser for configuring the BlogsStreamProcessor.
 * Utilizes the scopt library to define and parse command-line options, providing a user-friendly interface for setting up the processor.
 */
object BlogsWorkflowStreamConfig {

  val builder: OParserBuilder[BlogsWorkflowStreamConfig] = OParser.builder[BlogsWorkflowStreamConfig]

  val parser: OParser[Unit, BlogsWorkflowStreamConfig] = {

    import builder._

    OParser.sequence(
      programName("BlogsStreamApp"),
      head("BlogsStreamApp", "0.1"),

      opt[String]('d', "execDate")
        .action((x, c) => c.copy(execDate = x))
        .text("Execution date"),

      opt[String]('h', "kafkaHost")
        .action((x, c) => c.copy(kafkaHost = x))
        .text("Kafka hosts"),

      opt[String]('g', "kafkaConsumerGroup")
        .action((x, c) => c.copy(kafkaConsumerGroup = x))
        .text("Kafka Consumer Group"),

      opt[String]('t', "kafkaBlogsTopicName")
        .action((x, c) => c.copy(blogsTopicName = x))
        .text("Kafka Blogs topic name"),

      opt[String]('p', "hdfsPath")
        .action((x, c) => c.copy(hdfsPath = x))
        .text("Path to HDFS"),

      opt[String] ('o', "hdfsOffsetsPath")
        .action((x, c) => c.copy(hdfsOffsetsPath = x))
        .text("Path to Kafka offsets in the HDFS")
    )
  }
}