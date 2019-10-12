package com.bigdata.sql.n_01_getting_started.n_01_spark_sql_sparkSession

import com.bigdata.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(true)
    println(spark)

    spark.stop()
  }

}
