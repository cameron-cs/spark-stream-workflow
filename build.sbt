import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / scalaVersion := "2.11.12"

ThisBuild / organization := "org.cameron.cs"

ThisBuild / organizationName := "spark-stream-workflow"

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-Xfatal-warnings"
)

lazy val sparkVersion              = "2.4.4"
lazy val hadoopVersion             = "2.8.5"
lazy val kafkaVersion              = "1.0.2"
lazy val hbaseVersion              = "1.4.8"
lazy val esVersion                 = "7.8.1"
lazy val catsVersion               = "2.0.0"
lazy val scoptVersion              = "4.1.0"
lazy val prometheusClient          = "0.13.0"

lazy val projectSettings = Seq(
  version := "1.0.0",
  unmanagedBase := baseDirectory.value / "lib",
  resolvers ++= Seq(
    Resolver.mavenLocal,
    Resolver.sonatypeRepo("releases"),
    Resolver.DefaultMavenRepository
  ),
  libraryDependencies ++= Seq(
    ("org.apache.spark"             %% "spark-core"              % sparkVersion     % Provided )
      .exclude("org.slf4j", "slf4j-api")
      .exclude("org.slf4j", "slf4j-log4j12"),
    "org.apache.spark"              %% "spark-mllib"             % sparkVersion     % Provided ,
    "org.apache.spark"              %% "spark-hive"              % sparkVersion     % Provided ,
    "org.apache.spark"              %% "spark-sql"               % sparkVersion     % Provided ,
    "org.apache.spark"              %% "spark-core"              % sparkVersion     % Provided ,
    "org.apache.spark"              %% "spark-catalyst"          % sparkVersion     % Provided ,
    "org.apache.spark"              %% "spark-sql-kafka-0-10"    % sparkVersion     % Provided ,
    ("org.apache.kafka"              % "kafka-clients"           % kafkaVersion)
      .exclude("com.fasterxml.jackson.core", "*"),
    ("org.apache.kafka"             %% "kafka"                   % kafkaVersion)
      .exclude("com.fasterxml.jackson.core", "*"),
    "org.apache.hadoop"              % "hadoop-client"           % hadoopVersion    % Provided ,
    "com.github.scopt"               %% "scopt"                  % scoptVersion,
    "io.prometheus"                  % "simpleclient"            % prometheusClient,
    "io.prometheus"                  % "simpleclient_common"     % prometheusClient,
    "io.prometheus"                  % "simpleclient_httpserver" % prometheusClient
  )
)

lazy val testSettings = Seq(
  Test / unmanagedSourceDirectories += baseDirectory.value / "src" / "test",
  libraryDependencies += "org.scalatest"  %% "scalatest"  % "3.0.3"          % Test,
  libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.14.1"         % Test,
)

lazy val common = (project in file("common"))
  .settings(projectSettings)
  .settings(testSettings)
  .settings(
    organization := "org.cameron.cs",
    name := "common",
    version := "1.0",
    assembly / assemblyJarName := "common.jar",
    assembly / assemblyMergeStrategy := {
      case PathList(ps@_*) if ps.last.endsWith("src/main/resources") => MergeStrategy.discard
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = true, includeDependency = true)
  )

lazy val postsWorkflowStream = (project in file("posts_workflow_stream"))
  .settings(projectSettings)
  .settings(testSettings)
  .dependsOn(common)
  .settings(
    organization := "org.cameron.cs",
    name := "posts-workflow-stream",
    version := "1.0",
    assembly / assemblyJarName := "posts_workflow_stream.jar",
    assembly / assemblyMergeStrategy := {
      case PathList(ps@_*) if ps.last.endsWith("src/main/resources") => MergeStrategy.discard
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = true, includeDependency = true)
  )


lazy val blogsWorkflowStream = (project in file("blogs_workflow_stream"))
  .settings(projectSettings)
  .settings(testSettings)
  .dependsOn(common)
  .settings(
    organization := "org.cameron.cs",
    name := "blogs-workflow-stream",
    version := "1.0",
    assembly / assemblyJarName := "blogs_workflow_stream.jar",
    assembly / assemblyMergeStrategy := {
      case PathList(ps@_*) if ps.last.endsWith("src/main/resources") => MergeStrategy.discard
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = true, includeDependency = true)
  )


lazy val metricsWorkflowStream = (project in file("metrics_workflow_stream"))
  .settings(projectSettings)
  .settings(testSettings)
  .dependsOn(common)
  .settings(
    organization := "org.cameron.cs",
    name := "metrics-workflow-stream",
    version := "1.0",
    assembly / assemblyJarName := "metrics_workflow_stream.jar",
    assembly / assemblyMergeStrategy := {
      case PathList(ps@_*) if ps.last.endsWith("src/main/resources") => MergeStrategy.discard
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = true, includeDependency = true)
  )


lazy val streamsWorkflowMerger = (project in file("streams_workflow_merger"))
  .settings(projectSettings)
  .settings(testSettings)
  .dependsOn(common)
  .settings(
    organization := "org.cameron.cs",
    name := "streams-workflow-merger",
    version := "1.0",
    assembly / assemblyJarName := "streams_workflow_merger.jar",
    assembly / assemblyMergeStrategy := {
      case PathList(ps@_*) if ps.last.endsWith("src/main/resources") => MergeStrategy.discard
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = true, includeDependency = true)
  )

lazy val dataCleaner = (project in file("data_cleaner"))
  .settings(projectSettings)
  .settings(testSettings)
  .dependsOn(common)
  .settings(
    organization := "org.cameron.cs",
    name := "data-cleaner",
    version := "1.0",
    assembly / assemblyJarName := "data_cleaner.jar",
    assembly / assemblyMergeStrategy := {
      case PathList(ps@_*) if ps.last.endsWith("src/main/resources") => MergeStrategy.discard
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = true, includeDependency = true)
  )

lazy val root =
  (project in file("."))
    .enablePlugins(ScalaUnidocPlugin)
    .aggregate(common, postsWorkflowStream, blogsWorkflowStream, metricsWorkflowStream, streamsWorkflowMerger, dataCleaner)
    .settings(
      name := "spark-stream-workflow",
      ScalaUnidoc / unidoc / unidocProjectFilter := inAnyProject
    )