package sparkml.features

import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.SparkSession

object VectorIndexerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("VectorIndexerExample")
      .master("local")
      .getOrCreate()

    val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val indexer = new VectorIndexer()
      .setInputCol("sparkml/features")
      .setOutputCol("indexed")
      .setMaxCategories(10)

    val indexerModel = indexer.fit(data)
    val categoricalFeatures : Set[Int] = indexerModel.categoryMaps.keys.toSet
    println(s"Choose ${categoricalFeatures.size} categorical features: " + categoricalFeatures.mkString(", "))

    val indexedData = indexerModel.transform(data)
    indexedData.show()
  }
}
