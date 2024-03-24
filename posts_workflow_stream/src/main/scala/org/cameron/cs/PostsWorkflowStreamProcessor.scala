package org.cameron.cs

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.cameron.cs.common.CustomKeyGenerator
import org.cameron.cs.common.kafka.{SparkKafkaCustomOffsetsWriter, SparkManualCustomOffsetsManager}

import java.time.LocalTime
import java.time.format.DateTimeFormatter

class PostsWorkflowStreamProcessor(spark: SparkSession, conf: PostsWorkflowStreamConfig) extends Logging {

  import spark.implicits._

  /**
   * Reads a stream of posts from a specified Kafka topic using provided Kafka settings.
   * This function sets up a DataFrame to consume data from the beginning of the topic until the latest message.
   *
   * @param kafkaHost          The host address of the Kafka server.
   * @param kafkaConsumerGroup The consumer group ID for Kafka consumption.
   * @param postsTopicName     The name of the Kafka topic from which to read posts.
   * @return DataFrame representing the stream of posts from Kafka.
   */
  private def readPostsStream(kafkaHost: String,
                              kafkaConsumerGroup: String,
                              postsTopicName: String): DataFrame = {
    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)
      .option("subscribe", postsTopicName)
      .option("group.id", kafkaConsumerGroup)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("auto.offset.reset", "earliest")
      .option("failOnDataLoss", "false")
      .load()
  }

  /**
   * Reads a stream of posts from a Kafka topic starting from specified offsets.
   * This function is used to continue reading from a specific point in the Kafka topic, based on previously saved offsets.
   *
   * @param kafkaHost          The host address of the Kafka server.
   * @param kafkaConsumerGroup The consumer group ID for Kafka consumption.
   * @param postsTopicName     The name of the Kafka topic from which to read posts.
   * @param startingOffsets    The starting offsets as a JSON string for each partition of the Kafka topic.
   * @return DataFrame representing the stream of posts from Kafka starting from the given offsets.
   */
  private def readPostsStreamWithOffsets(kafkaHost: String,
                                         kafkaConsumerGroup: String,
                                         postsTopicName: String,
                                         startingOffsets: String): DataFrame =
    spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)
      .option("group.id", kafkaConsumerGroup)
      .option("subscribe", postsTopicName)
      .option("startingOffsets", startingOffsets)
      .option("endingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

  /**
   * Main processing function that orchestrates the data flow from Kafka to HDFS.
   * This function encompasses several steps: reading data from Kafka, either from the earliest offset or a specific checkpoint;
   * transforming the data as per the schema; and finally, writing the transformed data to HDFS.
   * It also manages Kafka offsets to ensure data consistency and fault tolerance.
   *
   * The process begins by attempting to read the latest saved offsets from HDFS. If this read is successful,
   * the function resumes reading from Kafka using these offsets, ensuring continuity of data processing.
   * In case of any issues (such as missing offset information), the function defaults to reading from the earliest available data in Kafka.
   *
   * Once the data is read from Kafka, it undergoes transformation. The raw Kafka message is parsed to extract relevant fields.
   * Additional fields are conditionally added based on the existence of nested columns within the data.
   * This step ensures the DataFrame matches the expected schema, adding nulls where data is not available.
   *
   * The final step involves writing the transformed data to HDFS. The location and format are defined in the configuration.
   * Additionally, the latest Kafka offsets are calculated and stored in HDFS. This step is crucial for ensuring that subsequent
   * runs of the processor start from the correct position in the Kafka topic.
   *
   * @note This function uses a combination of Spark SQL operations and Kafka's consumer API to achieve its objectives.
   *       It is designed to handle large streams of data efficiently while ensuring data consistency and fault tolerance.
   *
   * Processes the stream of posts from Kafka and writes the transformed data to HDFS.
   * This method manages the entire data pipeline - reading from Kafka, transforming data, and writing to HDFS.
   * It also handles offset management for Kafka to ensure exactly-once semantics.
   */
  def process(): Unit = {
    val execDate: String           = conf.execDate
    val kafkaHost: String          = conf.kafkaHost
    val kafkaConsumerGroup: String = conf.kafkaConsumerGroup
    val postsTopicName: String     = conf.postsTopicName
    val hdfsPath: String           = conf.hdfsPath
    val hdfsOffsetsPath: String    = conf.hdfsOffsetsPath

    var isFirstRun = true

    val postsStream =
      try {
        val offsetsParams =
          new SparkManualCustomOffsetsManager(spark)
            .getLatestOffsetsAsStr(hdfsOffsetsPath, execDate, kafkaHost, kafkaConsumerGroup, postsTopicName)

        isFirstRun = false
        readPostsStreamWithOffsets(kafkaHost, kafkaConsumerGroup, postsTopicName, s"""{"$postsTopicName": {$offsetsParams}}""")
      } catch {
        case t: Throwable =>
          logWarning(s"Something went wrong while reading the posts offset parquet file...", t)
          logInfo(s"Reading the posts stream in Kafka [hosts=${conf.kafkaHost}, topic=${conf.postsTopicName}]")
          readPostsStream(kafkaHost, kafkaConsumerGroup, postsTopicName)
      }

    val urlHashKey = udf { url: String => CustomKeyGenerator.generateKey(url) }

    val postsSchema = spark.read.json((postsStream select $"value").as[String]).schema

    val postsData =
      postsStream
        .withColumn("jsonData", from_json($"value" cast StringType, postsSchema))
        .withColumn("CurrentTimestamp", current_timestamp)

    val postsFlattened =
      postsData.select($"jsonData.*", $"CurrentTimestamp", $"partition" as "kafkapartition", $"offset" as "kafkaoffset")
        .withColumn("BlogMetrics", $"Blog.Metrics")
        .withColumn("topics", expr("transform(topics, t -> t.Id)"))

    val postsFinal =
      postsFlattened.select(
        $"Author.Url" as "authorurl",
        $"Author.Type" as "accounttype",
        $"Metrics.FollowerCount" as "accountfollowers",
        $"BlogansLanguage" as "bloganslanguage",
        $"BlogMetrics.FollowerCount" as "blogfollowers",
        $"Blog.UrlHash" as "blogid",
        $"Blog.Type" as "blogtype",
        $"Metrics.CommentCount" as "commentscount",
        urlHashKey($"Url") as "id",
        $"IsSpam" as "isspam",
        $"Metrics.LikeCount" as "likescount",
        $"Metrics.Timestamp" as "metrictimestamp",
        $"Parent.UrlHash" as "parentid",
        ($"CreateDate" cast TimestampType) as "partdate",
        $"Type" as "posttype",
        $"Url" as "posturl",
        $"Metrics.RepostCount" as "repostscount",
        $"Author.ProviderType" as "socialnetworktype",
        $"Topics" as "topics",
        $"Metrics.ViewCount" as "viewscount",
        $"CurrentTimestamp" as "currenttimestamp",
        $"kafkapartition" as "kafkapartition",
        $"kafkaoffset" as "kafkaoffset"
      )

    val curTime = LocalTime.now.format(DateTimeFormatter.ofPattern("HH-mm"))
    val dateTimePath = s"${execDate.replace("-", "")}/$curTime"

    new SparkKafkaCustomOffsetsWriter(spark)
      .saveOffsetsWithExecDate(postsFinal)(isFirstRun, postsTopicName, kafkaConsumerGroup, execDate, curTime, hdfsOffsetsPath)

    logInfo(s"Writing the posts data...")

    postsFinal
      .withColumn("execdate", lit(execDate))
      .write
      .partitionBy("id")
      .mode("overwrite")
      .parquet(s"$hdfsPath/$dateTimePath")
  }
}
