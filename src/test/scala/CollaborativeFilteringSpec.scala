import commons._

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS

import org.apache.spark.sql.SparkSession

import org.specs2._

import scala.util.control.Exception._

class CollaborativeFilteringSpec extends Specification with Stoppable {
  def is =
    s2"""

  Spark CollaborativeFiltering Spec

  CollaborativeFiltering
     Mean Squared Error                              $meanSquaredError
  """

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  var rmse: Double = _
  val spark = SparkSession.builder()
    .master("local")
    .appName("ALSExample")
    .getOrCreate()
  import spark.implicits._

  org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
  val ratings = spark.read.textFile("src/test/resources/sample_movielens_ratings.txt")
    .map(parseRating)
    .toDF()
  val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

  val als = new ALS()
    .setMaxIter(5)
    .setRegParam(0.01)
    .setUserCol("userId")
    .setItemCol("movieId")
    .setRatingCol("rating")
  val model = als.fit(training)

  val predictions = model.transform(test)

  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse")
    .setLabelCol("rating")
    .setPredictionCol("prediction")
  rmse = evaluator.evaluate(predictions)

  spark.stop()

  def meanSquaredError = rmse must beCloseTo(0.25, 0.01)
}

