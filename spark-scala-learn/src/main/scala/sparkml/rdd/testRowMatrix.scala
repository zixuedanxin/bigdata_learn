package sparkml.rdd

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}


object testRowMatrix {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("testRowMatrix")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("labeleddata.txt")
      .map(_.split(" ")
      .map(_.toDouble))
      .map(line => Vectors.dense(line))
    val rm = new RowMatrix(rdd)
    println(rm.numRows())
    println(rm.numCols())
  }
}
