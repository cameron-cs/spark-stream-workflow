package org.cameron.cs

import org.apache.hadoop.fs.{FileSystem, Path, Trash}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ArrayBuffer

class DataCleanerProcessor(spark: SparkSession, datePattern: String = "yyyy-MM-dd") extends Logging {

  def cleanData(lowerBound: String, upperBound: String, path: String, skipTrash: Boolean)(prefix: String): Unit = {
    logInfo(s"Starting cleaning the [$prefix] data")
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val formatter = DateTimeFormatter.ofPattern(datePattern)
    val startDate = LocalDate.parse(lowerBound, formatter)
    val endDate = LocalDate.parse(upperBound, formatter)

    val dates = ArrayBuffer[LocalDate]()

    var currentDate = startDate
    while (!currentDate.isAfter(endDate)) {
      dates += currentDate
      currentDate = currentDate.plusDays(1)
    }

    val dateStrings = dates.map(_.format(formatter))

    logInfo(s"Planning to remove the next dates: $dateStrings")
    for (date <- dateStrings) {
      val targetPath = s"$path/$date"
      val dirToDelete = new Path(targetPath)

      if (skipTrash) {
        logInfo(s"Skipping trash, deleting data permanently from [$targetPath], since the parameter 'skipTrash' = '$skipTrash'")
        hdfs.delete(dirToDelete, true)
      } else {
        logInfo(s"Moving deleted data to trash from [$targetPath]")
        val trash = new Trash(hdfs.getConf)
        if (!trash.moveToTrash(dirToDelete)) {
          logWarning(s"Failed to move [$targetPath] to Trash. Deleting it permanently.")
          hdfs.delete(dirToDelete, true)
        }
      }
    }
    hdfs.close()
  }
}
