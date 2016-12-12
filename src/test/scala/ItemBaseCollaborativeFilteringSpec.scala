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
  private val ratingTableName = "movie_ratings"
  sourceDataFrame.createOrReplaceTempView(s"$ratingTableName")

  """Recommender System""".stripMargin >> {
    """Positive case""".stripMargin >> {
      """User1 and User2 correlation""".stripMargin >> pending {
        val aggregateColumn = "user_id"
        val ratingTargetColumn = "movie_id"
        val user1 = 1
        val user2 = 2
        val answer = Recommender.getCorrelation(ratingTableName, aggregateColumn, ratingTargetColumn, user1, user2)

        answer must beCloseTo(-0.3, 0.1)
      }

      """Correlation ranking based on User similarity""".stripMargin >> pending {
        import spark.implicits._

        val aggregateColumn = "user_id"
        val ratingTargetColumn = "movie_id"
        val user1 = 1
        val correlations = Recommender.getCorrelations(ratingTableName, aggregateColumn, ratingTargetColumn, user1)
        val availables = correlations.filter($"correlation".isNaN =!= true and $"correlation" > 0.0)
        availables.describe().show(false)

        true must_== true
      }

      """Correlation ranking based on Movie similarity""".stripMargin >> pending {
        import spark.implicits._

        val aggregateColumn = "movie_id"
        val ratingTargetColumn = "user_id"
        val movie1 = 1
        val correlations = Recommender.getCorrelations(ratingTableName, aggregateColumn, ratingTargetColumn, movie1)
        val availables = correlations.filter($"correlation".isNaN =!= true and $"correlation" > 0.0)
        availables.describe().show(false)

        true must_== true
      }

      """Recommend item based on user similarity""".stripMargin >> pending {
        val aggregateColumn = "user_id"
        val ratingTargetColumn = "movie_id"
        val user1 = 1
        val itemBased = Recommender.getRecommendations(ratingTableName, aggregateColumn, ratingTargetColumn, user1)
        itemBased.describe().show(false)

        true must_== true
      }
    }

    """Negative case""".stripMargin >> {
      """Not exists users""".stripMargin >> pending {
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

  def printCodeInfo(): Unit = {
    val element = Thread.currentThread().getStackTrace()(2)
    val className = element.getClassName()
    val fileName = element.getFileName()
    val methodName = element.getMethodName()
    val lineNumber = element.getLineNumber()
    logger.info(s"$fileName:$lineNumber $className#$methodName")
  }
}
