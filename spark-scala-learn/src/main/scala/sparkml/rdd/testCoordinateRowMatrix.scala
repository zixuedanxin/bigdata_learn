package sparkml.rdd

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.{SparkConf, SparkContext}


object testCoordinateRowMatrix {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("testCoordinateRowMatrix")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("labeleddata.txt")
      .map(_.split(" ")
        .map(_.toDouble))
      .map(vue => (vue(0).toLong, vue(1).toLong, vue(2)))
      .map(vue2 => new MatrixEntry(vue2 _1, vue2 _2, vue2 _3))
    val crm = new CoordinateMatrix(rdd)
    crm.entries.foreach(println)
  }
}
