package com.bigdata.local.rdd.operation.dependency.narrow.n_02_RangeDependency

import com.bigdata.local.rdd.operation.base.BaseScalaSparkContext

object Run1 extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {
    val sc = pre()
    val rdd1 = sc.textFile("src/main/resource/log4j.properties",2)

    println(rdd1.collect().mkString("\n"))

    //rdd1.partitions(0).asInstanceOf[org.apache.spark.rdd.HadoopPartition]

    sc.stop()
  }

}