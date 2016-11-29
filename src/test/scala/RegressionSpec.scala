/**
  * Created by tomoya@couger.co.jp on 7/19/16.
  */

import commons._

import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}

import org.specs2._

import scala.util.control.Exception._

class RegressionSpec extends Specification with Stoppable {
  def is =
    s2"""

  Spark Regression Spec

  Regression
     Mean Absolute Error                              $meanAbsoluteError
  """

  case class Profile(marriedAge: Option[Double],
                     blood: String,
                     sex: String,
                     height: Double,
                     weight: Double)

  val customSchema = StructType(Seq(
    StructField("marriedAge", DoubleType, true),
    StructField("blood", StringType, true),
    StructField("sex", StringType, true),
    StructField("height", DoubleType, true),
    StructField("weight", DoubleType, true)
  ))
  val sparkConf = new SparkConf()
    .setAppName("RegressionSpec")
    .setMaster("local[1]")
    .setSparkHome(System.getenv("SPARK_HOME"))
  using(new SparkContext(sparkConf)) { sc =>
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val training = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(customSchema)
      .load("src/test/resources/married_regression_data.csv")

    val bloodIndexer = new StringIndexer()
      .setInputCol("blood")
      .setOutputCol("bloodIndex")
    val sexIndexer = new StringIndexer()
      .setInputCol("sex")
      .setOutputCol("sexIndex")

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "bloodIndex",
        "sexIndex",
        "height",
        "weight"
      ))
      .setOutputCol("features")

    val scaler = new StandardScaler()
      .setInputCol(assembler.getOutputCol)
      .setOutputCol("scaledFeatures")

    val regression = new LinearRegression()
      .setLabelCol("marriedAge")
      .setFeaturesCol(scaler.getOutputCol)

    val pipeline = new Pipeline()
      .setStages(Array(
        bloodIndexer,
        sexIndexer,
        assembler,
        scaler,
        regression
      ))

    val paramGrid = new ParamGridBuilder()
      .addGrid(regression.regParam, Array(0.1, 0.5, 0.01))
      .addGrid(regression.maxIter, Array(10, 100, 1000))
      .build()

    val evaluator = new RegressionEvaluator()
      .setLabelCol(regression.getLabelCol)
      .setPredictionCol(regression.getPredictionCol)

    val cross = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    val model = cross.fit(training)

    var test = sc.parallelize(Seq(
      // A型標準体型男
      Profile(None, "A", "男", 170, 65),
      // B型標準体型男
      Profile(None, "B", "男", 170, 65),
      // O型標準体型男
      Profile(None, "O", "男", 170, 65),
      // AB型標準体型男
      Profile(None, "AB", "男", 170, 65),
      // A型標準体型女
      Profile(None, "A", "女", 160, 50),
      // B型標準体型女
      Profile(None, "B", "女", 160, 50),
      // O型標準体型女
      Profile(None, "O", "女", 160, 50),
      // AB型標準体型女
      Profile(None, "AB", "女", 160, 50),
      // A型もやし男
      Profile(None, "A", "男", 170, 35),
      // A型でぶ男
      Profile(None, "A", "男", 170, 100),
      // A型もやし女
      Profile(None, "A", "女", 170, 35),
      // A型でぶ女
      Profile(None, "A", "女", 170, 100),
      // A型高身長男
      Profile(None, "A", "男", 190, 80),
      // A型小人(男)
      Profile(None, "A", "男", 17, 6),
      // A型巨人(男)
      Profile(None, "A", "男", 17000, 6500)
    )).toDF

    model.transform(test)
      .select("blood", "sex", "height", "weight", "prediction").show
  }

  def meanAbsoluteError = true must_== false
}
