package com.bigdata.local.rdd.operation.action.sortby.n_01_升序排序

import com.bigdata.local.rdd.operation.base.BaseScalaSparkContext

object TakeRun extends BaseScalaSparkContext{



  def main(args: Array[String]): Unit = {
    val sc = pre()
    val r1 = sc.parallelize(Array("A","V","B","V","C","V","W"),2)
    val r2= r1.sortBy(x => x,true)

    println(s"r2结果:"+ r2.collect().mkString(" "))



    sc.stop()
  }


}
