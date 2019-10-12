package com.bigdata.sql.n_02_spark_dataset.action.n_05_dataset_count

import com.bigdata.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()
    val dataSet = spark.read.textFile("src/main/resource/data/text/people.txt")
    println(dataSet.count())





    spark.stop()


  }
}

