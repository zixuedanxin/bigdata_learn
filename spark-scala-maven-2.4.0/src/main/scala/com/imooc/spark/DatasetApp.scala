package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
 * Dataset操作  ->读取scv文件
 */
object DatasetApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DatasetApp")
      .master("local[2]").getOrCreate()

    //注意：需要导入隐式转换
    import spark.implicits._

    val path = "file:///f:/text/sales.csv"

    //spark如何解析csv文件？
    val df = spark.read.option("header","true").option("inferSchema","true").csv(path)
    df.show

    val ds = df.as[Sales]
    //map是迭代，每一行只取出id
    ds.map(line => line.itemId).show


    spark.sql("seletc name from person").show

    //df.seletc("name")
    df.select("name")

    ds.map(line => line.itemId)

    spark.stop()
  }

  case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)
}
