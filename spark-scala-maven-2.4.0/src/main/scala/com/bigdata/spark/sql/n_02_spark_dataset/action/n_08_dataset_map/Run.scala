package com.bigdata.spark.sql.n_02_spark_dataset.action.n_08_dataset_map

import com.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("src/main/resource/data/text/people.txt")
    import spark.implicits._
    val lineWordLength = dataSet.map( line => line.split(" ").size)

    println(lineWordLength.collect().mkString("\n"))





    spark.stop()


  }
}

