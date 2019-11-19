package sparkml.rdd

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}


object testCorrect {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("testCorrect")
    val sc = new SparkContext(conf)
    val rddX = sc.textFile("x.txt")
      .flatMap(_.split(" ")
        .map(_.toDouble))
    val rddY = sc.textFile("y.txt")
      .flatMap(_.split(" ")
        .map(_.toDouble))
    val correlation: Double = Statistics.corr(rddX,rddY)
    println(correlation)
  }
}
