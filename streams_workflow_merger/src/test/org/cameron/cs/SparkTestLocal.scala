package org.cameron.cs

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

trait SparkTestLocal extends FunSuite with BeforeAndAfterAll {

  def testDataPathPrefix: String = ""

  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Stream merger unit tests")
    .getOrCreate()

  override def beforeAll(): Unit = spark.sparkContext.setLogLevel("ERROR")
}