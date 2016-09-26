name := "Spark MLlib 2.x examples"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-Xlint", "-deprecation", "-unchecked", "-feature", "-Xelide-below", "ALL")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-mllib" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "org.specs2" %% "specs2" % "3.7",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2" % "compile",
  "org.slf4j" % "slf4j-api" % "1.7.21" % "compile",
  "ch.qos.logback" % "logback-classic" % "1.1.7" % "runtime"
)
