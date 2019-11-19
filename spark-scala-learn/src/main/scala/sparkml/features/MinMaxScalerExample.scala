package sparkml.features

import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object MinMaxScalerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NormalizerExample")
      .master("local")
      .getOrCreate()

    val dataFrame = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0,0.1,-1.0)),
      (1, Vectors.dense(2.0, 1.1, 1.0)),
      (2, Vectors.dense(3.0, 10.1, 3.0))
    )).toDF("id", "sparkml/features")

    val scaler = new MinMaxScaler()
      .setInputCol("sparkml/features")
      .setOutputCol("scaledFeatures")

    val scalerModel = scaler.fit(dataFrame)

    val scaledData = scalerModel.transform(dataFrame)
    println(s"Features scaled to range [${scaler.getMin}, ${scaler.getMax}]")
    scaledData.select("sparkml/features","scaledFeatures").show()
  }
}
