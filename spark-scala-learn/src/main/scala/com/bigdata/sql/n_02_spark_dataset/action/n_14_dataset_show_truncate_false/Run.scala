package com.bigdata.sql.n_02_spark_dataset.action.n_14_dataset_show_truncate_false

import com.bigdata.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("src/main/resource/data/text/people.txt")

    /**
      * 以表格的形式显示前3行数据
      * numRows是显示前几行的数据
      * false 不进行返回行数据截断
      */

    val result = dataSet.show(10,false)
    println(result)





    spark.stop()


  }
}

