package com.bigdata.spark.sql.n_02_spark_dataset.action.n_07_dataset_foreach

import com.bigdata.spark.standalone.base.BaseSparkSession


object Run1$Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession(true)
    val dataSet = spark.read.textFile("src/main/resource/data/text/people.txt")
    val r2=dataSet.foreach(x=>x+"hud")
    dataSet.foreach(println(_))





    spark.stop()


  }
}

