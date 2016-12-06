import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification

import recommender.Recommender

/**
  * Created by tomoya@couger.co.jp on 2016/11/11.
  */
class ItemBaseCollaborativeFilteringSpec extends Specification {

  private val logger = LogManager.getLogger(this.getClass)

  private implicit val spark = SparkSession.builder()
    .master("local")
    .appName("ItemBaseCollaborativeFilteringSpec")
    .getOrCreate()

  private val filePath = "src/test/resources/movie_ratings.csv"
  private val sourceDataFrame = spark.sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)
    .withColumnRenamed("userId", "user_id")
    .withColumnRenamed("movieId", "movie_id")
  sourceDataFrame.createOrReplaceTempView("movie_ratings")

  """Recommender System""".stripMargin >> {
    """Positive case""".stripMargin >> {
      """User1 and User2 correlation""".stripMargin >> {
        val ratingTableName = "movie_ratings"
        val aggregateColumn = "user_id"
        val ratingTargetColumn = "movie_id"
        val user1 = 1
        val user2 = 2
        val answer = Recommender.getCorrelation(ratingTableName, aggregateColumn, ratingTargetColumn, user1, user2)

        answer must beCloseTo(-0.3, 0.1)
      }

      """User similarity ranking""".stripMargin >> pending {
        import spark.implicits._

        val user1 = 1
        val correlations = getCorrelations(user1)
        val availables = correlations.filter($"correlation".isNaN =!= true and $"correlation" > 0.0)
        availables.describe().show(false)

        true must_== true
      }

      """Item recommendation based on user similarity""".stripMargin >> pending {
        val user1 = 1
        val itemBased = itemRecommendationBasedOnUserSimilarity(user1)
        itemBased.describe().show()

        true must_== true
      }
    }

    """Negative case""".stripMargin >> {
      """Not exists users""".stripMargin >> {
        val ratingTableName = "movie_ratings"
        val aggregateColumn = "user_id"
        val ratingTargetColumn = "movie_id"
        val user1 = 0
        val user2 = 0
        val answer = Recommender.getCorrelation(ratingTableName, aggregateColumn, ratingTargetColumn, user1, user2)

        answer must beCloseTo(0.0, 0.001)
      }
    }
  }

  step {
    logger.info("final step")
    spark.stop()
  }

  def itemRecommendationBasedOnUserSimilarity(user1: Int) = {
    import spark.implicits._

    val correlations = getCorrelations(user1)
    val availables = correlations.filter($"correlation".isNaN =!= true and $"correlation" > 0.0)
    availables.createOrReplaceTempView("correlations")
    val notUser1s = spark.sql(
      s"""SELECT * FROM movie_ratings WHERE user_id <> $user1""".stripMargin)
    notUser1s.createOrReplaceTempView("not_user1s")
    val intermediateItemBased1 = spark.sql(
      s"""SELECT
         |a.movie_id,
         |(b.correlation * a.rating) as score,
         |b.correlation as correlation
         |FROM not_user1s as a
         |LEFT OUTER JOIN correlations as b ON a.user_id = b.user_id""".stripMargin)
    intermediateItemBased1.createOrReplaceTempView("intermediate_item_based1")
    spark.sql(
      s"""SELECT movie_id, (SUM(score) / SUM(correlation)) as recommendation FROM intermediate_item_based1
         |GROUP BY movie_id""".stripMargin)
  }

  def getCorrelations(user1: Int) = {
    import spark.implicits._

    val sqlNonUser1IDs = spark.sql(
      s"""SELECT DISTINCT user_id FROM movie_ratings WHERE user_id <> $user1""".stripMargin)
    val nonUser1IDs = sqlNonUser1IDs.select("user_id").map(_.getInt(0)).collect
    val ratingTableName = "movie_ratings"
    val aggregateColumn = "user_id"
    val ratingTargetColumn = "movie_id"
    val correlations = nonUser1IDs.map(userX => (userX, Recommender.getCorrelation(ratingTableName, aggregateColumn, ratingTargetColumn, user1, userX)))
    spark.sparkContext.parallelize(correlations).toDF("user_id", "correlation")
  }

  def printCodeInfo(): Unit = {
    val element = Thread.currentThread().getStackTrace()(2)
    val className = element.getClassName()
    val fileName = element.getFileName()
    val methodName = element.getMethodName()
    val lineNumber = element.getLineNumber()
    logger.info(s"$fileName:$lineNumber $className#$methodName")
  }
}
