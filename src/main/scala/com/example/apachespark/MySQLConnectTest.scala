package com.example.apachespark

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import recommender.Recommender

import scala.util.control.Exception._

import com.typesafe.config.ConfigFactory

/**
  * Created by tomoya on 2016/12/29.
  */
object MySQLConnectTest {
  def main(args: Array[String]): Unit = {
    val logger = LogManager.getLogger(this.getClass)

    val conf = ConfigFactory.load()

    implicit val spark = SparkSession.builder()
      .master("local")
      .appName("MySQLConnectTest")
      .getOrCreate()
    import spark.sqlContext.implicits._

    val options = Map(
      "driver" -> conf.getString("db.default.driver"),
      "url" -> conf.getString("db.default.url"),
      "user" -> conf.getString("db.default.user"),
      "password" -> conf.getString("db.default.password"),
      "dbtable" -> "ratings"
    )

    val ratings = spark.sqlContext.read.format("jdbc").options(options).load()
    val ratingTableName = "movie_ratings"
    ratings.createOrReplaceTempView(ratingTableName)

    val aggregateColumn = "user_id"
    val ratingTargetColumn = "movie_id"
    val user1 = 1
    val user2 = 2

    val answer = allCatch withApply { t: Throwable =>
      logger.error("Recommender.getCorrelation", t)
      0.0
    } andFinally {
      spark.stop()
    } apply {
      Recommender.getCorrelation(ratingTableName, aggregateColumn, ratingTargetColumn, user1, user2)
    }
    logger.info(s"user1, user2 correlation = ${answer}")
  }
}
