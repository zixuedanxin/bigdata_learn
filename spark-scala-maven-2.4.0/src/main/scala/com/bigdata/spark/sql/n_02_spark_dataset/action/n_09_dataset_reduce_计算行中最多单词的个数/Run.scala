package com.bigdata.spark.sql.n_02_spark_dataset.action.n_09_dataset_reduce_计算行中最多单词的个数

import com.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("src/main/resource/data/text/people.txt")

    /**
      * 计算行中最多单词的个数
      */
    import spark.implicits._
    val lineWordLength = dataSet.map( line => line.split(" ").size)
    val result = lineWordLength.reduce((a,b) => Math.max(a,b))

    println(result)





    spark.stop()


  }
}

