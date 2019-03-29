package com.bigdata.spark.local.rdd.operation.dependency.narrow.n_01_OneToOneDependency

import com.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run1 extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {
    val sc = pre()
    val rdd1 = sc.textFile("src/main/resource/data/text/people.txt",2)

    println(rdd1.collect().mkString("\n"))

    //rdd1.partitions(0).asInstanceOf[org.apache.spark.rdd.HadoopPartition]

    sc.stop()
  }

}