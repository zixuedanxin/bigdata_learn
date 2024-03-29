package com.bigdata.sql.n_02_spark_dataset.function.n_01_dataset_cache

import com.bigdata.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("src/main/resource/data/text/people.txt")

    dataSet.cache()

    val result = dataSet.head(10)

    println(result.mkString("\n"))






    spark.stop()


  }
}

