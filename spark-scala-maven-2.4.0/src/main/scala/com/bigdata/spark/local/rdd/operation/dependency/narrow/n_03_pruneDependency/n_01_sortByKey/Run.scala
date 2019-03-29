package com.bigdata.spark.local.rdd.operation.dependency.narrow.n_03_pruneDependency.n_01_sortByKey

import com.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object Run  extends BaseScalaSparkContext{

  def main(args: Array[String]): Unit = {

    val sc = pre()
    val rdd1 = sc.parallelize(List(("a",2),("d",1),("b",8),("d",3)),2)  //ParallelCollectionRDD
    val rdd1Sort = rdd1.sortByKey()   //ShuffleRDD

    println("rdd1Sort \n" + rdd1Sort.collect().mkString("\n"))


    sc.stop()
  }

}
