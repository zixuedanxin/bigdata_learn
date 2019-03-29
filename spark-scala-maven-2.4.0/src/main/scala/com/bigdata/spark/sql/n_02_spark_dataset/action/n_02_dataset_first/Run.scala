package com.bigdata.spark.sql.n_02_spark_dataset.action.n_02_dataset_first

import com.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()
    val dataSet = spark.read.textFile("src/main/resource/data/text/people.txt")

    println(dataSet.first()) //first里边调用的是head()
    spark.stop()
  }
}

