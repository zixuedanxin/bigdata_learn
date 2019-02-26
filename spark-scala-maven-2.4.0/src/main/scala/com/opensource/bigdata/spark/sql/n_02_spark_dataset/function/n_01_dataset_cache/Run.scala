package com.opensource.bigdata.spark.sql.n_02_spark_dataset.function.n_01_dataset_cache

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("/home/liuwen/data/word.big.cn.text")

    dataSet.cache()

    val result = dataSet.head(10)

    println(result.mkString("\n"))






    spark.stop()


  }
}

