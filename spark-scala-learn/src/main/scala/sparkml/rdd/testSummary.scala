package sparkml.rdd

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}


object testSummary {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("testIndexedRowMatrix")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("a.txt")
      .map(_.split(" ")
        .map(_.toDouble))
      .map(line => Vectors.dense(line))
    val summary = Statistics.colStats(rdd)
    println(summary.max)
    println(summary.min)
    println(summary.count)//行内数据个数
    println(summary.mean)//均值
    println(summary.numNonzeros)//非零数字个数
    println(summary.variance)//标准差
    println(summary.normL1)//欧式距离
    println(summary.normL2)//曼哈顿距离
  }
}
