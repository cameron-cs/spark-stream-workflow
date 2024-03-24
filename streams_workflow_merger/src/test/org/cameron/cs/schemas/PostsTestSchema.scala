package org.cameron.cs.schemas

import org.apache.spark.sql.types._

object PostsTestSchema {

  val schema = new StructType()
    .add("authorurl", StringType, nullable = true)
    .add("accounttype", LongType, nullable = true)
    .add("accountfollowers", LongType, nullable = true)
    .add("bloganslanguage", LongType, nullable = true)
    .add("blogfollowers", LongType, nullable = true)
    .add("blogid", StringType, nullable = true)
    .add("blogtype", LongType, nullable = true)
    .add("commentscount", LongType, nullable = true)
    .add("id", StringType, nullable = true)
    .add("isspam", BooleanType, nullable = true)
    .add("likescount", LongType, nullable = true)
    .add("metrictimestamp", StringType, nullable = true)
    .add("parentid", StringType, nullable = true)
    .add("partdate", TimestampType, nullable = true)
    .add("posttype", LongType, nullable = true)
    .add("posturl", StringType, nullable = true)
    .add("repostscount", LongType, nullable = true)
    .add("socialnetworktype", LongType, nullable = true)
    .add("viewscount", LongType, nullable = true)
    .add("currenttimestamp", TimestampType, nullable = true)
    .add("kafkapartition", IntegerType, nullable = true)
    .add("kafkaoffset", LongType, nullable = true)
    .add("execdate", StringType, nullable = true)

}