package com.bigdata.sql.n_09_spark_datasource.n_07_partitionBy

import com.bigdata.standalone.base.BaseSparkSession

object ReadRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)

    val sqlDF = spark.sql("select * from namesPartByColor.parquet")
    sqlDF.show

    spark.stop()
  }
}

// value write is not a member of Unit