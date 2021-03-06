name := "Spark MLlib 2.x examples"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-Xlint",
  "-deprecation",
  "-unchecked",
  "-feature",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  //  "-Ywarn-unused",
  //  "-Ywarn-unused-import",
  "-Ywarn-value-discard",
  "-Xelide-below", "ALL"
)

libraryDependencies ++= (Seq( // apache spark
  "org.apache.spark" %% "spark-core" % "2.0.2",
  "org.apache.spark" %% "spark-mllib" % "2.0.2",
  "org.apache.spark" %% "spark-sql" % "2.0.2",
  "com.databricks" %% "spark-csv" % "1.5.0"
) ++ Seq( // mysql
  "mysql" % "mysql-connector-java" % "5.1.38"
) ++ Seq( // slick
  "com.typesafe.slick" %% "slick" % "3.1.1",
  "org.slf4j" % "slf4j-nop" % "1.6.4"
) ++ Seq( // typesafe config
  "com.typesafe" % "config" % "1.3.1"
) ++ Seq( // specs2
  "org.specs2" %% "specs2" % "3.7" % "test"
) ++ Seq( // apache commons-io
  "org.apache.commons" % "commons-io" % "1.3.2"
) ++ Seq( // opencsv
  "net.sf.opencsv" % "opencsv" % "2.3"
))
