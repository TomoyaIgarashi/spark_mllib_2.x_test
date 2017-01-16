package com.example.apachespark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import recommender.Recommender

import scala.util.control.Exception._
import slick.driver.MySQLDriver.api._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by tomoya.igarashi on 2016/12/29.
  */
object GetRecommendation {
  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(this.getClass)

    val conf = ConfigFactory.load(args(0))

    val db1 = Database.forConfig("db.default", conf)
    val optJobIdUserId = allCatch withApply { t =>
      logger.error("Find waiting job error", t)
      None
    } andFinally {
      db1.close()
    } apply {
      val selectQuery = sql"""
        SELECT id, user_id FROM jobs WHERE status = "Accepted" ORDER BY created_at DESC LIMIT 1
      """.as[(Long, Long)]
      val selectFuture = db1.run(selectQuery)
      val xs = Await.result(selectFuture, 1.seconds)
      for {
        (jobId, userId) <- xs.headOption
      } yield {
        val updateQuery = sqlu"""
          UPDATE jobs SET status = "Running", updated_at = NOW() WHERE id = ${jobId}
        """
        val updateFuture = db1.run(updateQuery)
        Await.result(updateFuture, 1.seconds)
        (jobId, userId)
      }
    }

    val recommendations = for {
      (jobId, userId) <- optJobIdUserId
    } yield {
      implicit val spark = SparkSession.builder()
        .appName("GetRecommendation")
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

      allCatch withApply { t: Throwable =>
        logger.error("Recommender.getRecommendations", t)
        (jobId, userId, Array.empty[(Long, Double)])
      } andFinally {
        spark.stop()
      } apply {
        val xs = Recommender.getRecommendations(ratingTableName, aggregateColumn, ratingTargetColumn, userId)
        val ys: Array[(Long, Double)] = xs.filter(row => !row.anyNull).map(row => (row.getLong(0), row.getDouble(1))).collect()
        (jobId, userId, ys)
      }
    }

    for {
      (jobId, userId, xs) <- recommendations if xs.nonEmpty
    } {
      val db2 = Database.forConfig("db.default")
      allCatch withApply { t: Throwable =>
        logger.error("Recommendations set error", t)
      } andFinally {
        db2.close()
      } apply {
        val values = xs.map { t =>
          s"(${jobId}, ${userId}, ${t._1}, ${t._2}, NOW(), NOW())"
        }.mkString(", ")
        values match {
          case xs if xs.nonEmpty =>
            val insertQuery = sqlu"""
            INSERT INTO recommendations (job_id, user_id, movie_id, recommendation, created_at, updated_at)
            VALUES #${values}
          """
            val updateQuery = sqlu"""
            UPDATE jobs SET status = "Done", updated_at = NOW() WHERE id = ${jobId}
          """
            val transaction = DBIO.seq(insertQuery, updateQuery)
            val transactionFuture = db2.run(transaction.transactionally)
            Await.result(transactionFuture, 10.seconds)
          case _ =>
        }
      }
    }
  }
}
