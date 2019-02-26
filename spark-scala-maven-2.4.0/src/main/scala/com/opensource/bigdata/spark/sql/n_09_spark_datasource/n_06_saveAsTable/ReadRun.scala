package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_06_saveAsTable

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object ReadRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(false)

    val sqlDF = spark.sql("select * from people_bucketed")
    sqlDF.show

    spark.stop()
  }
}

// value write is not a member of Unit