import org.apache.log4j.LogManager
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession
import org.specs2.mutable.Specification

/**
  * Created by tomoya@couger.co.jp on 2016/11/11.
  */
class ItemBaseCollaborativeFilteringSpec extends Specification {

  private val logger = LogManager.getLogger(this.getClass)

  private val spark = SparkSession.builder()
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
      """User1 and User2 correlation""".stripMargin >> pending {
        val user1 = 1
        val user2 = 2
        val answer = getCorrelation(user1, user2)

        answer must beCloseTo(-0.3, 0.1)
      }

      """User similarity ranking""".stripMargin >> {
        import spark.implicits._

        val user1 = 1
        val correlations = getCorrelations(user1)
        val availables = correlations.filter($"correlation".isNaN =!= true and $"correlation" > 0.0)
        availables.describe().show(false)

        true must_== true
      }
    }

    """Negative case""".stripMargin >> {
      """Not exists users""".stripMargin >> pending {
        val user1 = 0
        val user2 = 0
        val answer = getCorrelation(user1, user2)

        answer must beCloseTo(0.0, 0.001)
      }
    }
  }

  step {
    logger.info("final step")
    spark.stop()
  }

  def getCorrelations(user1: Int) = {
    import spark.implicits._

    val sqlNonUser1IDs = spark.sql(
      s"""SELECT DISTINCT user_id FROM movie_ratings WHERE user_id <> $user1""".stripMargin)
    val nonUser1IDs = sqlNonUser1IDs.select("user_id").map(_.getInt(0)).collect
    val correlations = nonUser1IDs.map(userX => (userX, getCorrelation(user1, userX)))
    spark.sparkContext.parallelize(correlations).toDF("user_id", "correlation")
  }

  def getCorrelation(user1: Int, user2: Int): Double = {
    import spark.implicits._
    val sqlIntersectedMovieIDs = spark.sql(
      s"""SELECT movie_id FROM movie_ratings WHERE user_id = $user1
         |INTERSECT
         |SELECT movie_id FROM movie_ratings WHERE user_id = $user2""".stripMargin)
    val intersectedMovieIDs = (sqlIntersectedMovieIDs
      .select("movie_id")
      .map(_.getInt(0))
      .collect match {
      case a if a.length <= 1 => Array.empty[Int] // Cannot compute the covariance of a RowMatrix with <= 1 row.
      case a => a
    }).mkString(", ") match {
      case s if s.nonEmpty => Some(s)
      case _ => None
    }
    intersectedMovieIDs.map { s =>
      val user1Ratings = spark.sql(
        s"""SELECT movie_id, rating FROM movie_ratings
           |WHERE movie_id IN ($s)
           |AND user_id = $user1
           |ORDER BY movie_id""".stripMargin)
      val user2Ratings = spark.sql(
        s"""SELECT movie_id, rating FROM movie_ratings
           |WHERE movie_id IN ($s)
           |AND user_id = $user2
           |ORDER BY movie_id""".stripMargin)
      val xs = user1Ratings.select("rating").rdd.map(_.getDouble(0))
      val ys = user2Ratings.select("rating").rdd.map(_.getDouble(0))
      Statistics.corr(xs, ys, "pearson")
    }.getOrElse(0.0)
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
