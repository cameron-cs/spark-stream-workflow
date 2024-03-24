package org.cameron.cs.conf

import scopt.{OParser, OParserBuilder}

/**
 * Configuration class for the DataCleanerProcessor.
 *
 * @param lowerBound The lower bound date for the current processing job (for removing)
 * @param upperBound The upper bound date for the current processing job (for removing)
 * @param cleanerDataName An alias (name) for the current processing job 
 * @param hdfsDataPath Path in HDFS where the processed data should be removed.
 * @param skipTrash Indicates whether to skip the trash when deleting files. If "true", files will be deleted permanently.
 */
case class DataCleanerConfig(lowerBound: String           = "",
                             upperBound: String           = "",
                             cleanerDataName: String      = "",
                             hdfsDataPath: String         = "",
                             skipTrash: String            = "false")

object DataCleanerConfig {

  val builder: OParserBuilder[DataCleanerConfig] = OParser.builder[DataCleanerConfig]

  val parser: OParser[Unit, DataCleanerConfig] = {

    import builder._

    OParser.sequence(
      programName("DataCleanerConfig"),
      head("DataCleanerConfig", "0.1"),

      opt[String]('l', "lowerBound")
        .action((x, c) => c.copy(lowerBound = x))
        .text("Lower bound date"),

      opt[String]('u', "upperBound")
        .action((x, c) => c.copy(upperBound = x))
        .text("Upper bound date"),

      opt[String]('n', "cleanerDataName")
        .action((x, c) => c.copy(cleanerDataName = x))
        .text("Cleaner data name"),

      opt[String]('p', "hdfsDataPath")
        .action((x, c) => c.copy(hdfsDataPath = x))
        .text("Path to HDFS"),

      opt[String]('s', "skipTrash")
        .action((x, c) => c.copy(skipTrash = x))
        .text("Skip trash (HDFS)")
    )
  }
}