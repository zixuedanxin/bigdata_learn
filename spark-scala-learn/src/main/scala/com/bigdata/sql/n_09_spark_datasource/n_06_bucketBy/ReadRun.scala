package com.bigdata.sql.n_09_spark_datasource.n_06_bucketBy

import com.bigdata.standalone.base.BaseSparkSession

object ReadRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(false)

    val sqlDF = spark.sql("select * from people_bucketed3")
    sqlDF.show

    spark.stop()
  }
}

// value write is not a member of Unit