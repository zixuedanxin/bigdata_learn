package com.bigdata.sql.n_02_spark_dataset.action.n_09_dataset_reduce

import com.bigdata.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("src/main/resource/data/text/people.txt")

    /**
      * 统计所有行单词个数
      */
    import spark.implicits._
    val lineWordLength = dataSet.map( line => line.split(" ").size)
    val result = lineWordLength.reduce((a,b) => a + b)

    println(result)





    spark.stop()


  }
}

