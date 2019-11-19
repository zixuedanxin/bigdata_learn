package com.xuzh.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession


object TFIDFeature {

  case class Love(id: Long, text: String, label: Double)

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder().master("local")
      .appName("TFIDFeature")
      .enableHiveSupport().getOrCreate()

    spark.sparkContext.setCheckpointDir("D:\\原始数据\\sparkMl")

    // 1.训练样本
    val sentenceData = spark.createDataFrame(
      Seq(
        Love(1L, "I love you", 1.0),
        Love(2L, "There is nothing to do", 0.0),
        Love(3L, "Work hard and you will success", 0.0),
        Love(4L, "We love each other", 1.0),
        Love(5L, "Where there is love, there are always wishes", 1.0),
        Love(6L, "I love you not because who you are,but because who I am when I am with you", 1.0),
        Love(7L, "Never frown,even when you are sad,because youn ever know who is falling in love with your smile", 1.0),
        Love(8L, "Whatever is worth doing is worth doing well", 0.0),
        Love(9L, "The hard part isn’t making the decision. It’s living with it", 0.0),
        Love(10L, "Your happy passer-by all knows, my distressed there is no place hides", 0.0),
        Love(11L, "When the whole world is about to rain, let’s make it clear in our heart together", 0.0)
      )
    ).toDF()

    sentenceData.show(false)

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(20)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("rawFeatures")

    // 通过CountVectorizer也可以获得词频向量
    val idf = new IDF()
      .setInputCol(hashingTF.getOutputCol)
      .setOutputCol("sparkml/features")

    val wordsData = tokenizer.transform(sentenceData)
    val featurizedData = hashingTF.transform(wordsData)
    val idfModel = idf.fit(featurizedData)

    // 3. 文档的向量化表示
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "sparkml/features").show(false)


  }
}
