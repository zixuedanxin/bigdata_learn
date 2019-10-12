package com.xuzh.spark

import org.apache.spark.sql.SparkSession

/**
 * SparkSession的使用=》2.0以后spark的入口点
 */
object SparkSessionApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkSessionApp")
      .master("local[2]").getOrCreate()

    val people = spark.read.json("file:///Users/rocky/data/people.json")
    people.show()

    spark.stop()
  }
}
