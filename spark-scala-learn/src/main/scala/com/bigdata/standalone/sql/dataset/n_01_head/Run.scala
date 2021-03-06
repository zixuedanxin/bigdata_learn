package com.bigdata.standalone.sql.dataset.n_01_head

import com.bigdata.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{


  appName = "Dataset head"

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(false,false,false)
    //返回dataFrame
    val df = spark.read.textFile("data/text/line.txt")
    val result = df.head(3)

    println(s"运行结果: ${result.mkString("\n")}")




    spark.stop()
  }

}
