/**
  * Created by tomoya@couger.co.jp on 7/19/16.
  */

import commons._

import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.specs2._

import scala.util.control.Exception._

class CosineSimilaritySpec extends Specification with Stoppable {
  def is =
    s2"""

  Spark CosineSimilarity Spec

  CosineSimilarity
     Mean Absolute Error                              $meanAbsoluteError
  """

  var mae: Double = _
  val sparkConf = new SparkConf()
    .setAppName("CosineSimilaritySpec")
    .setMaster("local[1]")
    .setSparkHome(System.getenv("SPARK_HOME"))
  using(new SparkContext(sparkConf)) { sc =>
    val data = sc.textFile("src/test/resources/cosine_similarity_data.txt")
    val rows = data.map { line =>
      val ds = line.split(" ").map(_.toDouble)
      Vectors.dense(ds)
    }.cache()
    val mat = new RowMatrix(rows)

    val exact = mat.columnSimilarities()
    val approx = mat.columnSimilarities(0.1)

    val exactEntries = exact.entries.map { case MatrixEntry(i, j, u) => ((i, j), u) }
    val approxEntries = approx.entries.map { case MatrixEntry(i, j, v) => ((i, j), v) }

    val MAE = exactEntries.leftOuterJoin(approxEntries).values.map {
      case (u, Some(v)) =>
        math.abs(u - v)
      case (u, None) =>
        math.abs(u)
    }.mean()

    mae = MAE
  }

  def meanAbsoluteError = mae must closeTo(0.05, 0.01)
}
