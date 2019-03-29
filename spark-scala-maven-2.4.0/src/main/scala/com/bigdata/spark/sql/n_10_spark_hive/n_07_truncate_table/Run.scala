package com.bigdata.spark.sql.n_10_spark_hive.n_07_truncate_table

import com.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(false,true)

    import spark.sql

    sql("CREATE database IF NOT EXISTS test_tmp")
    sql("use test_tmp")
    sql("CREATE TABLE IF NOT EXISTS student(name VARCHAR(64), age INT)")
    sql("INSERT INTO TABLE student  VALUES ('小王', 35), ('小李', 50)")
    sql("select * from student").show
//    +----+---+
//    |name|age|
//    +----+---+
//    |小王| 35|
//    |小李| 50|
//    +----+---+

    sql("truncate table student ")
    sql("select * from student").show

    //    +----+---+
    //    |name|age|
    //    +----+---+
    //    +----+---+

    spark.stop()
  }

}
