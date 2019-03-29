package com.bigdata.spark.local.sql.dataset.n_01_textFile.head

import com.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.textFile("file:///"+ getProjectPath +"/src/main/resource/data/text/people.txt")
    val result = df.head(1)
    df.show()
    println(s"运行结果: ${result.mkString("\n")}")


    spark.stop()
  }

}
