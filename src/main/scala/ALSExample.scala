package org.apache.spark.examples.ml

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * An example demonstrating ALS.
 * Run with
 * {{{
 * bin/run-example ml.ALSExample
 * }}}
 */
object ALSExample extends App {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .master("local")
    .appName("ALSExample")
    .getOrCreate()
  import spark.implicits._

  val ratings = spark.read.textFile("src/test/resources/sample_movielens_ratings.txt")
    .map(parseRating)
    .toDF()

  // Stats
  val groupByMovieId = ratings.groupBy("movieId")
  groupByMovieId.count().sort($"count").show()
  groupByMovieId.avg("rating").sort($"avg(rating)").show()

  val movieId8 = ratings.filter($"movieId" === 8)
  movieId8.show()
  movieId8.groupBy("movieId").sum().show()
  movieId8.groupBy("movieId").avg("rating").show()

  movieId8.describe("rating").show()

  val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

  // Build the recommendation model using ALS on the training data
  val als = new ALS()
    .setMaxIter(5)
    .setRegParam(0.01)
    .setUserCol("userId")
    .setItemCol("movieId")
    .setRatingCol("rating")
  val model = als.fit(training)

  // Evaluate the model by computing the RMSE on the test data
  val predictions = model.transform(test)
  
  predictions.printSchema
  predictions.show(false)

  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse")
    .setLabelCol("rating")
    .setPredictionCol("prediction")
  val rmse = evaluator.evaluate(predictions)
  println(s"Root-mean-square error = $rmse")

  spark.stop()
}
