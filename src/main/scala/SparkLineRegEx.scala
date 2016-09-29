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

  // Pipeline$B$N3FMWAG$r:n@.(B

  // $B!Z(B1$B![AG@-$N%Y%/%H%k$r@8@.$7$^$9(B
  val assembler = new VectorAssembler()
    .setInputCols(Array("x")) // $B@bL@JQ?t$r;XDj$7$^$9!#(B
    .setOutputCol("features") // $BJQ498e$NCM$r(Bfeatures$B$H$$$&L>A0$G%G!<%?%U%l!<%`$KDI2C(B
 
  // $B!Z(B2$B![AG@-$N%Y%/%H%k$rB?9`<0$K$7$^$9(B
  // $B7P83!&4*$,I,MW$JItJ,!#(B
  // $B$I$s$JB?9`<0$K$9$l$P%H%l!<%K%s%0%G!<%?$K%U%#%C%H$9$k$+$O$d$C$F$_$J$$$H$o$+$i$J$$$?$a(B
  val polynomialExpansion = new PolynomialExpansion()
    .setInputCol(assembler.getOutputCol) // features$B$NJ8;zNs$G$9!#(B
    .setOutputCol("polyFeatures") // $BJQ498e$NCM$r(BpolyFeatures$B$H$$$&L>A0$G%G!<%?%U%l!<%`$KDI2C(B
    .setDegree(4) // 4$B<!$NB?9`<0$G$9!#(B

  // $B!Z(B3$B![@~7A2s5"$NM=B,4o$r;XDj$7$^$9(B
  val linearRegression = new LinearRegression()
    .setLabelCol("y") // $BL\E*JQ?t$G$9!#(B
    .setFeaturesCol(polynomialExpansion.getOutputCol) // polyFeatures$B$NJ8;zNs$G$9!#(B
    .setMaxIter(100) // $B7+$jJV$7$,(B100$B2s(B
    .setRegParam(0.0) // $B@5B'2=%Q%i%a!<%?(B

  // $B!Z(B1$B![(B- $B!Z(B3$B![$r85$K%Q%$%W%i%$%s%*%V%8%'%/%H$r:n$j$^$9(B
  val pipeline = new Pipeline()
    .setStages(Array(assembler, polynomialExpansion, linearRegression))

  val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
  val model = pipeline.fit(trainingData) // $B3X=,%G!<%?$r;XDj$7$^$9!#(B

  // csv$B$KJ]B8(B
  val outputFilePath = args(1)
  model.transform(testData) // $B%F%9%H%G!<%?$r;XDj$7$^$9(B
    .select("x", "prediction")
    .write
    .format("com.databricks.spark.csv") // $B%G!<%?$N%U%)!<%^%C%H$r;XDj$7$^$9!#(B
    .option("header", "false") // $B%X%C%@!<$K%+%i%`L>$r$D$1$k$+(B
    .save(outputFilePath) // $B%U%!%$%k$NJ]B8@h$G$9!#(B

  df.describe("x").show()
  df.describe("y").show()

  args.foreach(myLogger.info)

  spark.stop()
}
