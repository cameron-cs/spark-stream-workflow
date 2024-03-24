package org.cameron.cs

import org.apache.hadoop.fs.{FileSystem, Path, Trash}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.cameron.cs.metrics.MetricsSchema
import org.cameron.cs.posts.{PostPartition, PostsSchema}

/**
 * Processor for merging blog and post metrics data streams.
 *
 * @param spark The Spark session to use.
 * @param conf Configuration parameters for the processor.
 */
class StreamsWorkflowMergerProcessor(spark: SparkSession, conf: StreamsWorkflowMergerConfig) extends Logging {

  import spark.implicits._

  /**
   * Merges blog data for a specific execution date.
   *
   * @param execDate        The execution date for which to merge blog data.
   * @param prevExecDate    The previous execution date to compare changes.
   * @param blogsPath       Path to the source blog data.
   * @param mergedBlogsPath Path to store merged blog data.
   */
  def mergeBlogs(execDate: String,
                 prevExecDate: String,
                 blogsPath: String,
                 mergedBlogsPath: String): Unit = {
    logInfo(s"Reading the blogs deltas for exec_date = $execDate")
    val blogsDelta = spark.read.parquet(s"$blogsPath/$execDate/*")

    val windowPartitionByAccId =
      Window
        .partitionBy("accountid")
        .orderBy($"currenttimestamp".desc)

    val rnBlogsDelta = blogsDelta.withColumn("rn", row_number().over(windowPartitionByAccId))
    val latestBlogsDelta = rnBlogsDelta.filter($"rn" === 1).drop("rn").withColumn("mergedate", lit(execDate))

    val finalData =
      try {
        logInfo(s"Reading the historic blog data for exec_date = $execDate")
        val historyData = spark.read.parquet(s"$mergedBlogsPath/$prevExecDate")

        historyData.union(latestBlogsDelta)
          .withColumn("rn", row_number().over(windowPartitionByAccId))
          .filter($"rn" === 1)
          .drop("rn")
      } catch {
        case e: Throwable =>
          logWarning("Couldn't read the blogs history in HDFS... First run mode", e)
          latestBlogsDelta
      }

    logInfo(s"Saving the newest historic blog data for exec_date = $execDate")
    finalData
      .repartition($"accountid" % 255)
      .write
      .mode("overwrite")
      .parquet(s"$mergedBlogsPath/$execDate")
  }

  private def writePartitionedByDatePosts(postPartitionDF: DataFrame, path: String): Unit =
    postPartitionDF
      .write
      .mode("overwrite")
      .parquet(path)

  def mergeDeltaPostsWithDeltaMetrics(execDate: String,
                                      postsDelta: DataFrame,
                                      metricsDelta: DataFrame,
                                      lowerBound: String): DataFrame = {
    val metricsRenamed = metricsDelta.columns.foldLeft(metricsDelta)((df, colName) =>
      if (postsDelta.columns.contains(colName)) df.withColumnRenamed(colName, colName + "_metrics")
      else df
    ).withColumnRenamed("id", "metrics_id")
      .withColumn("partdate_date_metrics", to_date($"partdate_metrics", "yyyy-MM-dd"))
      .filter(($"partdate_date_metrics" >= lowerBound) && ($"partdate_date_metrics" <= execDate))

    postsDelta.as("pd")
      .join(
        metricsRenamed.as("md"),
        ($"pd.id" === $"md.id_metrics") && ($"pd.partdate_date" === $"md.partdate_date_metrics"),
        "full_outer"
      ).withColumn("partdate_pmd", when($"pd.partdate_date".isNull, $"md.partdate_date_metrics").otherwise($"pd.partdate_date"))
      .withColumn("post_id", when($"pd.id".isNull, $"md.id_metrics").otherwise($"pd.id"))
      .withColumn("commentscount", coalesce($"comments", $"commentscount"))
      .withColumn("likescount", coalesce($"likes", $"likescount"))
      .withColumn("repostscount", coalesce($"reposts", $"repostscount"))
      .withColumn("viewscount", coalesce($"views", $"viewscount"))
      .drop("partdate_date", "partdate_date_metrics", "partdate_metrics", "partdate", "id", "id_metrics", "comments", "likes", "reposts", "views")
  }

  def joinPostDeltasWithPostHistory(postDelta: DataFrame, postHistory: DataFrame): DataFrame =
    postHistory.join(
      postDelta.as("pmd"),
      postHistory("post_id") === postDelta("post_id"),
      "full_outer"
    ).select(
      when($"pmd.authorurl".isNotNull, $"pmd.authorurl").otherwise($"data_to_upd.authorurl").alias("authorurl"),
      when($"pmd.accounttype".isNotNull, $"pmd.accounttype").otherwise($"data_to_upd.accounttype").alias("accounttype"),
      when($"pmd.accountfollowers".isNotNull, $"pmd.accountfollowers").otherwise($"data_to_upd.accountfollowers").alias("accountfollowers"),
      when($"pmd.bloganslanguage".isNotNull, $"pmd.bloganslanguage").otherwise($"data_to_upd.bloganslanguage").alias("bloganslanguage"),
      when($"pmd.blogfollowers".isNotNull, $"pmd.blogfollowers").otherwise($"data_to_upd.blogfollowers").alias("blogfollowers"),
      when($"pmd.blogid".isNotNull, $"pmd.blogid").otherwise($"data_to_upd.blogid").alias("blogid"),
      when($"pmd.blogtype".isNotNull, $"pmd.blogtype").otherwise($"data_to_upd.blogtype").alias("blogtype"),
      coalesce($"pmd.commentscount", $"data_to_upd.commentscount").alias("updated_commentscount").alias("updated_commentscount"),
      when($"pmd.isspam".isNotNull, $"pmd.isspam").otherwise($"data_to_upd.isspam").alias("isspam"),
      coalesce($"pmd.likescount", $"data_to_upd.likescount").alias("updated_likescount").alias("updated_likescount"),
      when($"pmd.metrictimestamp".isNotNull, $"pmd.metrictimestamp").otherwise($"data_to_upd.metrictimestamp").alias("metrictimestamp"),
      when($"pmd.parentid".isNotNull, $"pmd.parentid").otherwise($"data_to_upd.parentid").alias("parentid"),
      when($"pmd.posttype".isNotNull, $"pmd.posttype").otherwise($"data_to_upd.posttype").alias("posttype"),
      when($"pmd.posturl".isNotNull, $"pmd.posturl").otherwise($"data_to_upd.posturl").alias("posturl"),
      coalesce($"pmd.repostscount", $"data_to_upd.repostscount").alias("updated_repostscount").alias("updated_repostscount"),
      when($"pmd.socialnetworktype".isNotNull, $"pmd.socialnetworktype").otherwise($"data_to_upd.socialnetworktype").alias("socialnetworktype"),
      when($"pmd.topics".isNotNull, $"pmd.topics").otherwise($"data_to_upd.topics").alias("topics"),
      coalesce($"pmd.viewscount", $"data_to_upd.viewscount").alias("updated_viewscount").alias("updated_viewscount"),
      when($"pmd.currenttimestamp".isNotNull, $"pmd.currenttimestamp").otherwise($"data_to_upd.currenttimestamp").alias("currenttimestamp"),
      when($"pmd.kafkapartition".isNotNull, $"pmd.kafkapartition").otherwise($"data_to_upd.kafkapartition").alias("kafkapartition"),
      when($"pmd.kafkaoffset".isNotNull, $"pmd.kafkaoffset").otherwise($"data_to_upd.kafkaoffset").alias("kafkaoffset"),
      when($"pmd.execdate".isNotNull, $"pmd.execdate").otherwise($"data_to_upd.execdate").alias("execdate"),
      when($"pmd.blogtype_metrics".isNotNull, $"pmd.blogtype_metrics").otherwise($"data_to_upd.blogtype_metrics").alias("blogtype_metrics"),
      when($"pmd.blogurl".isNotNull, $"pmd.blogurl").otherwise($"data_to_upd.blogurl").alias("blogurl"),
      when($"pmd.commentnegativecount".isNotNull, $"pmd.commentnegativecount").otherwise($"data_to_upd.commentnegativecount").alias("commentnegativecount"),
      when($"pmd.commentpositivecount".isNotNull, $"pmd.commentpositivecount").otherwise($"data_to_upd.commentpositivecount").alias("commentpositivecount"),
      when($"pmd.metricstimestamp".isNotNull, $"pmd.metricstimestamp").otherwise($"data_to_upd.metricstimestamp").alias("metricstimestamp"),
      when($"pmd.metricstype".isNotNull, $"pmd.metricstype").otherwise($"data_to_upd.metricstype").alias("metricstype"),
      when($"pmd.metricsurl".isNotNull, $"pmd.metricsurl").otherwise($"data_to_upd.metricsurl").alias("metricsurl"),
      when($"pmd.currenttimestamp_metrics".isNotNull, $"pmd.currenttimestamp_metrics").otherwise($"data_to_upd.currenttimestamp_metrics").alias("currenttimestamp_metrics"),
      when($"pmd.kafkapartition_metrics".isNotNull, $"pmd.kafkapartition_metrics").otherwise($"data_to_upd.kafkapartition_metrics").alias("kafkapartition_metrics"),
      when($"pmd.kafkaoffset_metrics".isNotNull, $"pmd.kafkaoffset_metrics").otherwise($"data_to_upd.kafkaoffset_metrics").alias("kafkaoffset_metrics"),
      when($"pmd.execdate_metrics".isNotNull, $"pmd.execdate_metrics").otherwise($"data_to_upd.execdate_metrics").alias("execdate_metrics"),
      when($"pmd.partdate_pmd".isNotNull, $"pmd.partdate_pmd").otherwise($"data_to_upd.partdate_pmd").alias("partdate_pmd"),
      when($"pmd.post_id".isNotNull, $"pmd.post_id").otherwise($"data_to_upd.post_id").alias("post_id")
    ).drop("commentscount", "likescount", "repostscount", "viewscount")
      .withColumnRenamed("updated_commentscount", "commentscount")
      .withColumnRenamed("updated_likescount", "likescount")
      .withColumnRenamed("updated_repostscount", "repostscount")
      .withColumnRenamed("updated_viewscount", "viewscount")


  def makePostPartitions(postDatePartitions: Seq[String],
                         postsWithMetrics: DataFrame,
                         hdfs: FileSystem,
                         mergedPostsPath: String,
                         postsDeltaPath: String,
                         metricsDeltaPath: String,
                         execDate: String): Seq[PostPartition] = {
    val tempBasePath = s"$mergedPostsPath/temp/$execDate"

    val windowPartitionByPostIdTs =
      Window
        .partitionBy("post_id", "metrictimestamp")
        .orderBy($"metricstimestamp".desc_nulls_last, $"currenttimestamp".desc_nulls_last)

    postDatePartitions.map { date =>
      val partitionPath = s"$mergedPostsPath/partdate=$date"
      val hdfsPath = new Path(partitionPath)

      val (pathExists, isDir, nonEmpty) =
        try {
          (hdfs.exists(hdfsPath), hdfs.isDirectory(hdfsPath), hdfs.listStatus(hdfsPath).nonEmpty)
        } catch {
          case _: Exception => (false, false, false)
        }

      val postsWithMetricsDelta = postsWithMetrics.where($"partdate_pmd" === date)
      val postsDf =
        if (pathExists && isDir && nonEmpty) {
          logInfo(s"""Trying to read the partitioned posts data in HDFS: ["$partitionPath"]""")
          val postsHistory = spark.read.parquet(partitionPath).as("data_to_upd")
          logDebug(s"Trying to join the partitioned posts data in HDFS with the current deltas")
          joinPostDeltasWithPostHistory(postsWithMetricsDelta, postsHistory)
        } else {
          logWarning(s""""Something went wrong while trying to read the posts history in HDFS: ["$partitionPath"]""")
          logDebug(s"""Using the current merged Posts with Metrics data in HDFS : [paths: (postsPath = "$postsDeltaPath", metricsPath = "$metricsDeltaPath")]""")
          postsWithMetricsDelta
        }

      val tempPathOpt = if (pathExists) Some(s"$tempBasePath/partdate_pmd=$date") else None
      val finalPosts =
        postsDf
          .withColumn("rn", row_number().over(windowPartitionByPostIdTs))
          .filter($"rn" === 1)
          .drop("rn")

      PostPartition(
        partDate = date,
        path = hdfsPath,
        dataframe = finalPosts,
        exists = pathExists,
        tempPath = tempPathOpt
      )
    }
  }

  /**
   * Merges the latest posts and metrics data for a given execution date.
   * This function reads the deltas (changes) of posts and metrics data from the specified paths,
   * processes them to obtain the most recent data for each post, and then writes the merged
   * results to a specified location. Optionally, it handles the deletion of old data,
   * either moving it to the trash or deleting it permanently based on the `skipTrash` parameter.
   *
   * The merging process involves joining the posts and metrics data based on post IDs and
   * execution dates, filtering by a lower bound date, and resolving any overlapping or
   * duplicate data by retaining only the most recent records.
   *
   * @param execDate        The execution date representing the current processing batch. It's used
   *                        to locate the delta files for both posts and metrics data.
   * @param postsPath       The path to the directory containing the posts data.
   * @param mergedPostsPath The path to the directory where the merged posts data will be written.
   * @param metricsPath     The path to the directory containing the metrics data.
   * @param lowerBound      The lower bound date for filtering the data. Records older than this date
   *                        are not considered in the merging process.
   * @param skipTrash       A boolean flag that determines how old data is deleted. If true, old data
   *                        is deleted permanently; if false, it's moved to the trash.
   * @param format          The format of the source data files (default is "parquet").
   * @note This function requires the existence of a SparkSession (`spark`) with necessary
   *       configurations for file system interactions. The function reads and writes data using
   *       the Hadoop FileSystem API.
   * @note The function assumes the data schema is predefined and available in `MetricsSchema`
   *       and `PostsSchema`.
   * @example To merge posts and metrics data for the execution date "2023-01-01":
   * {{{
   *          val mergerProcessor = new StreamsMergerProcessor(spark, config)
   *          mergerProcessor.mergePostsAndMetrics("2023-01-01", "/path/to/posts", PostsSchema.schema,
   *                                               "/path/to/mergedPosts", "/path/to/metrics", MetricsSchema.schema,
   *                                               "2022-12-31", false)
   *           }}}
   */
  def mergePostsAndMetrics(execDate: String,
                           postsPath: String,
                           postsSchema: StructType,
                           mergedPostsPath: String,
                           metricsPath: String,
                           metricsSchema: StructType,
                           lowerBound: String,
                           skipTrash: Boolean,
                           format: String = "parquet"): Unit = {
    val datePattern = "yyyy-MM-dd"

    val metricsDeltaPath = s"$metricsPath/$execDate/*"
    logInfo(s"""Reading the metrics deltas in HDFS: ["$metricsDeltaPath"]""")
    val metricsDelta = spark.read.schema(metricsSchema).format(format).load(metricsDeltaPath)

    val postsDeltaPath = s"$postsPath/$execDate/*"
    logInfo(s"""Reading the posts deltas in HDFS: ["$postsDeltaPath"]""")
    val postsDelta = spark.read.schema(postsSchema).format(format).load(postsDeltaPath)
      .withColumn("partdate_date", to_date($"partdate", datePattern))
      .filter(($"partdate_date" >= lowerBound) && ($"partdate_date" <= execDate))

    logInfo(s"""Merging the posts deltas ($execDate) with the metrics deltas ($execDate)""")
    val postsWithMetricsRaw = mergeDeltaPostsWithDeltaMetrics(execDate, postsDelta, metricsDelta, lowerBound)

    val windowPartitionByPostId =
      Window
        .partitionBy("post_id")
        .orderBy($"metricstimestamp".desc, $"metrictimestamp".desc)

    val postsWithMetrics =
      postsWithMetricsRaw
        .withColumn("rn", row_number().over(windowPartitionByPostId))
        .filter($"rn" === 1)
        .drop("rn")

    val postDatePartitions = postsWithMetrics.select($"partdate_pmd").distinct().as[String].collect().toList.sorted
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val postsPartitioned = makePostPartitions(postDatePartitions, postsWithMetrics, hdfs, mergedPostsPath, postsDeltaPath, metricsDeltaPath, execDate)

    postsPartitioned.foreach { postPartition =>
      val tempPath = postPartition.tempPath
      val targetPath = postPartition.path

      if (postPartition.exists) {
        val tempPathHdfs = tempPath.get
        logInfo(s"""Saving the newest posts data to the temporary path [tempPath = "$tempPathHdfs"]""")
        writePartitionedByDatePosts(postPartition.dataframe, tempPathHdfs)

        logInfo(s"""Moving the posts data [from="$tempPath", to="$targetPath"]""")
        if (skipTrash) {
          logInfo(s"Skipping trash, deleting data permanently from [$targetPath], since the parameter 'skipTrash' = '$skipTrash'")
          hdfs.delete(targetPath, true)
        } else {
          logInfo(s"Moving deleted data to trash from [$targetPath]")
          val trash = new Trash(hdfs.getConf)
          if (!trash.moveToTrash(targetPath)) {
            logWarning(s"Failed to move [$targetPath] to Trash. Deleting it permanently.")
            hdfs.delete(targetPath, true)
          }
        }
        val tempPathSource = new Path(tempPathHdfs)
        hdfs.rename(tempPathSource, targetPath)
      } else {
        logInfo(s"""Writing the newest posts data ["$targetPath"]""")
        writePartitionedByDatePosts(postPartition.dataframe, s"$targetPath")
      }
    }
  }

  /**
   * -d (execDate): Execution date for the Spark job.
   *     - If 'exec_date' is provided when triggering the DAG manually, this value will be used.
   *     - If not provided, defaults to one day before the current DAG's execution date.
   *
   * --prevExecDate: Previous execution date for the Spark job.
   *     - Allows manual specification of a previous execution date when triggering the DAG.
   *     - If not provided, defaults to one day before the current DAG's execution date.
   *
   * --lowerBound: Lower bound date for processing data.
   *     - Allows manual specification of a lower bound date when triggering the DAG.
   *     - If not provided, defaults to 30 days before the current DAG's execution date.
   *
   * -p (postsPath): Path to the HDFS location for posts.
   *     - Specified via the Airflow variable 'hdfsPostsPath'.
   *
   * --po: Path to the HDFS location for offset data of posts.
   *     - Specified via the Airflow variable 'hdfsOffsetsPostsPath'.
   *
   * -b (blogsPath): Path to the HDFS location for blogs.
   *     - Specified via the Airflow variable 'hdfsBlogsPath'.
   *
   * --bo: Path to the HDFS location for offset data of blogs.
   *     - Specified via the Airflow variable 'hdfsOffsetsBlogsPath'.
   *
   * -m (metricsPath): Path to the HDFS location for metrics.
   *     - Specified via the Airflow variable 'hdfsMetricsPath'.
   *
   * --mo: Path to the HDFS location for offset data of metrics.
   *     - Specified via the Airflow variable 'hdfsOffsetsMetricsPath'.
   *
   * --mb (mergedBlogsPath): Path to the HDFS location for merged blog data.
   *     - Specified via the Airflow variable 'hdfsMergedBlogsPath'.
   *
   * --mp (mergedPostsPath): Path to the HDFS location for merged post data.
   *     - Specified via the Airflow variable 'hdfsMergedPostsPath'.
   *
   * --skipTrash: Flag to skip the trash when performing operations.
   *     - Specified as a boolean value via the Airflow variable 'hdfsSkipTrash'.
   *
   *  For manual DAG runs, parameters ('exec_date' and 'prev_exec_date') are necessary to pass while triggering the Airflow DAG:
   *  @example { "exec_date": "2023-12-20", "prev_exec_date": "2023-12-19" }
   */
  def process(): Unit = {
    processMergeBlogs()

    processMergePosts()
  }

  def processMergeBlogs(): Unit = {
    val execDate: String = conf.execDate.replace("-", "")
    val prevExecDate: String = conf.prevExecDate.replace("-", "")
    val blogsPath = conf.blogsPath
    val mergedBlogsPath = conf.mergedBlogsPath

    logInfo(s"execDate: ${conf.execDate}")
    logInfo(s"prevExecDate: ${conf.prevExecDate}")
    logInfo(s"blogsPath: ${conf.blogsPath}")
    logInfo(s"mergedBlogsPath: ${conf.mergedBlogsPath}")

    try {
      logInfo(s"Starting the process of merging blogs... [execDate = $execDate, prevExecDate = $prevExecDate, blogsPath = $blogsPath, mergedBlogsPath = $mergedBlogsPath]")
      mergeBlogs(execDate, prevExecDate.replace("-", ""), blogsPath, mergedBlogsPath)
    } catch {
      case e: Throwable =>
        logWarning(s"Something went wrong while processing merging blogs...", e)
        throw e
    }
  }

  def processMergePosts(): Unit = {
    val execDate: String = conf.execDate.replace("-", "")
    val lowerBound: String = conf.lowerBound.replace("-", "")
    val postsPath = conf.postsPath
    val metricsPath = conf.metricsPath
    val mergedPostsPath = conf.mergedPostsPath
    val skipTrash = conf.skipTrash.toLowerCase.toBoolean

    logInfo(s"execDate: ${conf.execDate}")
    logInfo(s"lowerBound: ${conf.lowerBound}")
    logInfo(s"postsPath: ${conf.postsPath}")
    logInfo(s"metricsPath: ${conf.metricsPath}")
    logInfo(s"mergedPostsPath: ${conf.mergedPostsPath}")
    logInfo(s"skipTrash: ${conf.skipTrash.toLowerCase.toBoolean}")

    try {
      logInfo(s"Starting to process of merging posts with metrics... [execDate = $execDate, postsPath = $postsPath, mergedPostsPath = $mergedPostsPath, metricsPath = $metricsPath, lowerBound = $lowerBound, skipTrash = $skipTrash]")
      mergePostsAndMetrics(execDate, postsPath, PostsSchema.schema, mergedPostsPath, metricsPath, MetricsSchema.schema, lowerBound, skipTrash)
    } catch {
      case e: Throwable =>
        logWarning(s"Something went wrong while processing merging posts with metrics...", e)
        throw e
    }
  }
}
