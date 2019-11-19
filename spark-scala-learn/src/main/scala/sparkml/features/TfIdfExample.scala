package sparkml.features

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

object TfIdfExample {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder()
      .master("local")
      .appName("TfIdfExample")
      .getOrCreate()
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label","sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val hashingTF =new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
//    featurizedData.select("rawFeatures").foreach(println(_))

//    val countVectorizer = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures")
//    val featurizedData = countVectorizer.fit(wordsData)
//    featurizedData.transform(wordsData).select("rawFeatures").foreach(println(_))
//      [(16,[0,2,3,5,6],[1.0,1.0,1.0,1.0,1.0])]
//      [(16,[0,4,7,8,10,13,14],[1.0,1.0,1.0,1.0,1.0,1.0,1.0])]
//      [(16,[1,9,11,12,15],[1.0,1.0,1.0,1.0,1.0])]

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("sparkml/features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "sparkml/features").foreach(println(_))
  }
}
