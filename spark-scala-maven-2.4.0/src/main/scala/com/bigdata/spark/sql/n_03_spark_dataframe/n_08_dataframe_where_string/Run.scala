package com.bigdata.spark.sql.n_03_spark_dataframe.n_08_dataframe_where_string

import com.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")


    df.where("age > 20").show()
//    +---+-----+
//    |age| name|
//    +---+-----+
//    | 30| Andy|
//    | 30|Think|
//    +---+-----+


    spark.stop()
  }

}
