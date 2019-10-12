package com.bigdata.sql.n_02_spark_dataset.action.n_15_dataset_show_truncate_vertical

import java.io.File

import com.bigdata.standalone.base.BaseSparkSession
import org.apache.spark.sql.SparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = sparkSession()
      //SparkSession.builder
      //.appName("Spark Pi").master("local")
      //.config("spark.sql.warehouse.dir",warehouseLocation)
      //.getOrCreate()
    val dataSet = spark.read.textFile("src/main/resource/data/text/people.txt")

    /**
      * 以表格的形式显示前10行数据
      *
      * @param numRows Number of rows to show
      * @param truncate If set to more than 0, truncates strings to `truncate` characters and
      *                    all cells will be aligned right.
      * @param vertical If set to true, prints output rows vertically (one line per column value).
      */

    val result = dataSet//.show(10,100,false)
    dataSet.show(10)
    println(result)





    spark.stop()


  }
}

