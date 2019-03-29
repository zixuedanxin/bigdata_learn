package com.bigdata.spark.sql.n_02_spark_dataset.action.n_18_dataset_toLocalIterator

import com.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("src/main/resource/data/text/people.txt")



    val result = dataSet.toLocalIterator()
    while (result.hasNext) println(result.next())







    spark.stop()


  }
}

