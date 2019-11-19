package sparkml.features

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession

object CountVectorizerExample {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder()
      .master("local")
      .appName("Word2VecExample")
      .getOrCreate()

    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c","d")),
      (1, Array("a", "b", "b", "c", "a","d","d"))
    )).toDF("id", "words")

    val cvModel : CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("sparkml/features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)
   //也可以使用先验词汇
    val cvm = new CountVectorizerModel(Array("a","b","c"))
      .setInputCol("words")
      .setOutputCol("result")

    cvModel.transform(df).show(false)
//    cvm.transform(df).show(false)
  }
}
