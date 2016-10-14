import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
 
import org.apache.log4j.Logger
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object SparkPCAEx extends App {
  val myLogger = LogManager.getLogger("myLogger")
  val spark = SparkSession.builder()
    .master("local")
    .appName("Spark PCA Example")
    .getOrCreate()

  import spark.implicits._

  val data = Array(
    Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
    Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
    Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
  )
  val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
  val pca = new PCA()
    .setInputCol("features")
    .setOutputCol("pcaFeatures")
    .setK(3)
    .fit(df)
  val pcaDF = pca.transform(df)
  val result = pcaDF.select("pcaFeatures")
  result.show(false)

  spark.stop()
}
