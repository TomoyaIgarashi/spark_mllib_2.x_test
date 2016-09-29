import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.LinearRegression
 
import org.apache.log4j.Logger
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object SparkLineRegEx extends App {
  val myLogger = LogManager.getLogger("myLogger")
  val spark = SparkSession.builder()
    .master("local")
    .appName("Spark LinearRegression Example")
    .getOrCreate()

  import spark.implicits._

  val filePath = args(0)
  val df = spark.sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)

  // Pipelineの各要素を作成

  // 【1】素性のベクトルを生成します
  val assembler = new VectorAssembler()
    .setInputCols(Array("x")) // 説明変数を指定します。
    .setOutputCol("features") // 変換後の値をfeaturesという名前でデータフレームに追加
 
  // 【2】素性のベクトルを多項式にします
  // 経験・勘が必要な部分。
  // どんな多項式にすればトレーニングデータにフィットするかはやってみないとわからないため
  val polynomialExpansion = new PolynomialExpansion()
    .setInputCol(assembler.getOutputCol) // featuresの文字列です。
    .setOutputCol("polyFeatures") // 変換後の値をpolyFeaturesという名前でデータフレームに追加
    .setDegree(4) // 4次の多項式です。

  // 【3】線形回帰の予測器を指定します
  val linearRegression = new LinearRegression()
    .setLabelCol("y") // 目的変数です。
    .setFeaturesCol(polynomialExpansion.getOutputCol) // polyFeaturesの文字列です。
    .setMaxIter(100) // 繰り返しが100回
    .setRegParam(0.0) // 正則化パラメータ

  // 【1】- 【3】を元にパイプラインオブジェクトを作ります
  val pipeline = new Pipeline()
    .setStages(Array(assembler, polynomialExpansion, linearRegression))

  val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
  val model = pipeline.fit(trainingData) // 学習データを指定します。

  // csvに保存
  val outputFilePath = args(1)
  model.transform(testData) // テストデータを指定します
    .select("x", "prediction")
    .write
    .format("com.databricks.spark.csv") // データのフォーマットを指定します。
    .option("header", "false") // ヘッダーにカラム名をつけるか
    .save(outputFilePath) // ファイルの保存先です。

  df.describe("x").show()
  df.describe("y").show()

  args.foreach(myLogger.info)

  spark.stop()
}
