package com.bigdata.local.sql.dataset.n_02_textFile.count

import com.bigdata.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.textFile("file:///"+ getProjectPath +"/src/main/resource/data/text/people.txt")
    println(s"===结果:${df.count()}")

//    ===结果:4






    spark.stop()
  }

}
