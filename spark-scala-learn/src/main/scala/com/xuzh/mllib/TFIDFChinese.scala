package com.xuzh.mllib

import com.xuzh.utils.HashingTF
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TFIDFChinese {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder().master("local")
      .appName("TFIDFChinese")
      .enableHiveSupport().getOrCreate()

    val path = "D:\\原始数据\\news\\*"
    val rdd = spark.sparkContext.wholeTextFiles(path) //读取目录下所有文件:[(filename1:content1),(filename2:context2)]
    val filename = rdd.map(_._1); //保存所有文件名称：[filename1,filename2]，为输出结果做准备

    import scala.collection.JavaConverters._
    val stopWords = spark.sparkContext.textFile("D:\\原始数据\\zh-stopWords.txt").collect().toSeq.asJava //构建停词

    val filter = new StopRecognition().insertStopWords(stopWords) //过滤停词
    filter.insertStopNatures("w", null, "null") //根据词性过滤
    val splitWordRdd = rdd.map(file => {
      //使用中文分词器将内容分词：[(filename1:w1 w3 w3...),(filename2:w1 w2 w3...)]
      val str = ToAnalysis.parse(file._2).recognition(filter).toStringWithOutNature(" ")
      (file._1, str.split(" "))
    })
    val df = spark.createDataFrame(splitWordRdd).toDF("fileName", "words");

    //val tokenizer = new org.apache.spark.ml.feature.RegexTokenizer().setInputCol("context").setOutputCol("words").setMinTokenLength(3)
    //val words = tokenizer.transform(df)
    //val stopWordsRemover = new org.apache.spark.ml.feature.StopWordsRemover().setInputCol("words").setOutputCol("stopWords");
    //val stopWords = stopWordsRemover.transform(df);
    //stopWords.select("stopWords").show(10, 200)
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(200000)
    val documents = hashingTF.transform(df)
    val idf = new org.apache.spark.ml.feature.IDF().setInputCol("rawFeatures").setOutputCol("sparkml/features")
    val idfModel = idf.fit(documents)
    val idfData = idfModel.transform(documents)
    var wordMap = df.select("words").rdd.flatMap {
      row => {
        //保存所有单词以及经过hasingTf后的索引值：{单词1:索引,单词2:索引...}
        row.getAs[Seq[String]](0).map {
          w => (hashingTF.indexOf(w), w)
        }
      }
    }.collect().toMap
    val keyWords = idfData.select("sparkml/features").rdd.map {
      x =>
        {
          val v = x.getAs[org.apache.spark.ml.linalg.SparseVector](0) //idf结果以稀疏矩阵保存
          v.indices.zip(v.values).sortWith((a, b) => {
            a._2 > b._2
          }).take(10).map(x => (wordMap.get(x._1).get, x._2)) //根据idf值从大到小排序，取前10个，并通过索引反查到词
        }
    }
    //[(文章1的关键词索引1:tf-idf值,文章1的关键词索引2:tf-idf值),(文章n的关键词索引1:tf-idf值,文章n的关键词索引2:tf-idf值)...],每组()表示一个新闻的关键词

    filename.zip(keyWords).collect().foreach(x => {
      println(x._1)
      x._2.foreach(x => println(x._1 + ":" + x._2 + " "))
    })


  }

}
