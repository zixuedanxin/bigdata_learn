package com.bigdata.spark.sql.n_02_spark_dataset.action.n_10_dataset_reduce_单词数量最多的行数据

import com.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("src/main/resource/data/text/people.txt")

    /**
      * 单词个数最多的行
      */

    val result = dataSet.reduce((a,b) => {
      if(a.split(" ").size > b.split(" ").size) a  else b
    })

    println(result)





    spark.stop()


  }
}

