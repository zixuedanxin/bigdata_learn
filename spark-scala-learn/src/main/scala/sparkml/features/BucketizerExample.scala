package sparkml.features

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SparkSession

object BucketizerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("BucketizerExample")
      .getOrCreate()

    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)
    val data = Array(-0.5,-0.3,0.0,0.2,0.8)
    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("sparkml/features")

    val bucketizer = new Bucketizer()
      .setInputCol("sparkml/features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    val bucketedData = bucketizer.transform(dataFrame)
    bucketedData.show()
  }
}
