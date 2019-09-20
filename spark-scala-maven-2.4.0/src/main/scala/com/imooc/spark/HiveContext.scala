package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * HiveContext的使用
  */
object HiveContext {

  def main(args: Array[String]): Unit = {
    val path = args(0)
    //1)创建相应的Context
    val sparkConf = new SparkConf()
    //在测试或者生产中，AppName和Master我们是通过脚本指定的
    //sparkConf.setAppName("HiveContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //2)相关的处理:表

    hiveContext.table("emp").show


    //3)关闭资源
    sc.stop()
  }

}
