package com.bigdata.spark.sql.n_10_spark_hive.n_08_drop_table

import com.bigdata.spark.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(false,true)

    import spark.sql
    sql("use test_tmp")
    sql("drop table student ")



    spark.stop()
  }

}
