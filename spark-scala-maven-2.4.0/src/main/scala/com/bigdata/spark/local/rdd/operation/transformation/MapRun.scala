package com.bigdata.spark.local.rdd.operation.transformation

import com.bigdata.spark.local.rdd.operation.base.BaseScalaSparkContext

object MapRun extends BaseScalaSparkContext{


  def main(args: Array[String]): Unit = {
    val sc = pre()
    val r1 = sc.parallelize(Array("A","V","B","V","C","V","W"),2)
    val r2 = r1.map(_+"aa").collect().mkString("\n")

    println("结果:"+ r2)



    sc.stop()
  }


}