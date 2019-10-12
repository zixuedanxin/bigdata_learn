package com.xuzh.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadHiveData {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("spark://master:7077").setAppName("ReadHiveData")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.sql("use test")
    val df = spark.sql("select * from user")
    df.show()

    spark.stop()

  }

}
