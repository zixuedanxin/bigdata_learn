package com.imooc

import org.apache.spark.sql.SparkSession

/**
  * SparkSession的使用
  */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSessionApp")
         .master("local[2]").getOrCreate()

    val text = spark.read.json("file:///f:/text")
    text.show()

    spark.stop()

  }
}
