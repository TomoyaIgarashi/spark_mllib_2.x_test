package recommender

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession

/**
  * Created by tomoya.igarashi on 2016/12/05.
  */
object Recommender {
  def getCorrelations(ratingTableName: String, aggregateColumn: String, ratingTargetColumn: String, aggregate1: Int)(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val sqlNonAggregate1IDs = sparkSession.sql(
      s"""SELECT DISTINCT $aggregateColumn FROM $ratingTableName WHERE $aggregateColumn <> $aggregate1""".stripMargin)
    val nonAggregate1IDs = sqlNonAggregate1IDs.select(s"$aggregateColumn").map(_.getInt(0)).collect
    val correlations = nonAggregate1IDs.map(aggregateOther => (aggregateOther, Recommender.getCorrelation(ratingTableName, aggregateColumn, ratingTargetColumn, aggregate1, aggregateOther)))
    sparkSession.sparkContext.parallelize(correlations).toDF(s"$aggregateColumn", "correlation")
  }

  def getCorrelation(ratingTableName: String, aggregateColumn: String, ratingTargetColumn: String,
                     aggregate1: Int, aggregate2: Int)(implicit sparkSession: SparkSession): Double = {
    import sparkSession.implicits._

    val sqlIntersectedRatingTargetIDs = sparkSession.sql(
      s"""SELECT $ratingTargetColumn FROM $ratingTableName WHERE $aggregateColumn = $aggregate1
         |INTERSECT
         |SELECT $ratingTargetColumn FROM $ratingTableName WHERE $aggregateColumn = $aggregate2""".stripMargin)
    val intersectedRatingTargetIDs = (sqlIntersectedRatingTargetIDs
      .select(s"$ratingTargetColumn")
      .map(_.getInt(0))
      .collect match {
      case a if a.length <= 1 => Array.empty[Int] // Cannot compute the covariance of a RowMatrix with <= 1 row.
      case a => a
    }).mkString(", ") match {
      case s if s.nonEmpty => Some(s)
      case _ => None
    }
    intersectedRatingTargetIDs.map { s =>
      val aggregate1Ratings = sparkSession.sql(
        s"""SELECT $ratingTargetColumn, rating FROM $ratingTableName
           |WHERE $ratingTargetColumn IN ($s)
           |AND $aggregateColumn = $aggregate1
           |ORDER BY $ratingTargetColumn""".stripMargin)
      val aggregate2Ratings = sparkSession.sql(
        s"""SELECT $ratingTargetColumn, rating FROM $ratingTableName
           |WHERE $ratingTargetColumn IN ($s)
           |AND $aggregateColumn = $aggregate2
           |ORDER BY $ratingTargetColumn""".stripMargin)
      val xs = aggregate1Ratings.select("rating").rdd.map(_.getDouble(0))
      val ys = aggregate2Ratings.select("rating").rdd.map(_.getDouble(0))
      Statistics.corr(xs, ys, "pearson")
    }.getOrElse(0.0)
  }


}
