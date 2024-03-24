package org.cameron.cs

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.cameron.cs.metrics.MetricsSchema

class PostsMetricsMergerTest extends SparkTestLocal {

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val dataPath = getClass.getClassLoader.getResource("data").getPath

  val hdfsPostsPath = s"$dataPath/postsMocks/csv"
  val hdfsOffsetsPostsPath = s"$dataPath/offsets/posts/csv"
  val hdfsBlogsPath = s"$dataPath/blogs/csv"
  val hdfsOffsetsBlogsPath = s"$dataPath/offsets/blogs/csv"
  val hdfsMetricsPath = s"$dataPath/metrics/csv"
  val hdfsOffsetsMetricsPath = s"$dataPath/offsets/metrics/csv"
  val hdfsMergedBlogsPath = s"$dataPath/merged/blogs/csv"
  val hdfsMergedPostsPath = s"$dataPath/merged/postsMocks/csv"

  val hdfsMockPostsPath = s"$dataPath/mock/posts"
  val hdfsMockMetricsPath = s"$dataPath/mock/metrics"
  val hdfsMockResultPath = s"$dataPath/mock/result"

  val conf: StreamsWorkflowMergerConfig = StreamsWorkflowMergerConfig(
      execDate = "20231130",
      prevExecDate = "20231130",
      lowerBound = "2023-11-01",
      postsPath = hdfsPostsPath,
      postsOffsetsPath = hdfsOffsetsPostsPath,
      metricsPath = hdfsMetricsPath,
      metricsOffsetsPath = hdfsOffsetsBlogsPath,
      blogsPath = hdfsBlogsPath,
      blogsOffsetsPath = hdfsOffsetsMetricsPath,
      mergedBlogsPath = hdfsMergedBlogsPath,
      mergedPostsPath = hdfsMergedPostsPath
  )

  val processor = new StreamsWorkflowMergerProcessor(spark, conf)

  def loadMockPostsDeltaCsv()(implicit spark: SparkSession): DataFrame =
    spark.read.format("csv").option("header", "true").load(hdfsMockPostsPath)

  def loadMockMetricsDeltaCsv()(implicit spark: SparkSession): DataFrame =
    spark.read.format("csv").schema(MetricsSchema.schema).load(hdfsMockMetricsPath).where($"blogurl".isNotNull)

  def loadMockResultsCsv()(implicit spark: SparkSession): DataFrame =
    spark.read.format("csv").option("header", "true").load(hdfsMockResultPath)

  test("merge posts with metrics on the first run") {
    val lowerBound = "2023-11-26"
    val execDate = "2023-12-26"

    val postsDeltaMock = loadMockPostsDeltaCsv().withColumn("partdate_date", to_date($"partdate", "yyyy-MM-dd"))
      .filter(($"partdate_date" >= lowerBound) && ($"partdate_date" <= execDate))
    val metricsDeltaMock = loadMockMetricsDeltaCsv()
    val resultsMock = loadMockResultsCsv()

    val mergedDeltasPostsMetricsRaw = processor.mergeDeltaPostsWithDeltaMetrics(execDate, postsDeltaMock, metricsDeltaMock, lowerBound).drop("currenttimestamp_metrics")

    val windowPartitionByPostId =
      Window
        .partitionBy("post_id")
        .orderBy($"metricstimestamp".desc, $"metrictimestamp".desc)

    val mergedDeltasPostsMetrics =
      mergedDeltasPostsMetricsRaw
        .withColumn("rn", row_number().over(windowPartitionByPostId))
        .filter($"rn" === 1)
        .drop("rn")

    assert(mergedDeltasPostsMetrics.columns.toSet == resultsMock.columns.toSet)

    // iterate over columns
    for (column <- mergedDeltasPostsMetrics.columns) {
      // cast the column to string and collect the values for the current column from both DataFrames
      val values1 = mergedDeltasPostsMetrics.select(col(column).cast("string")).collect().map(_.getString(0))
      val values2 = resultsMock.select(col(column).cast("string")).collect().map(_.getString(0))

      // iterate over rows and compare values
      val differences = values1.zip(values2).zipWithIndex.filter {
        case ((value1, value2), index) =>
          if (value1 == null && value2 == null) {
            false
          } else if (value1 != value2) {
            fail(s"Difference found in row $index for column '$column': $value1 vs $value2")
            true
          } else {
            false
          }
      }

      // if there are no differences, print a message
      if (differences.isEmpty)
        println(s"All values in column '$column' are identical.")
    }
  }

  override protected def afterAll(): Unit = spark.stop()
}
