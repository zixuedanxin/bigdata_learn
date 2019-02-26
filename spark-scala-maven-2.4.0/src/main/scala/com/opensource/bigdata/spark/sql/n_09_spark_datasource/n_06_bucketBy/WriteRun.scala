package com.opensource.bigdata.spark.sql.n_09_spark_datasource.n_06_bucketBy

import com.opensource.bigdata.spark.standalone.base.BaseSparkSession

object WriteRun extends BaseSparkSession{

  def main(args: Array[String]): Unit = {
    val spark = sparkSession(true)

    val sqlDF = spark.read.format("json").load("hdfs://standalone.com:9000/home/liuwen/output/json/employ.json")
    sqlDF.show
    //+----+-------+
    //| age|   name|
    //+----+-------+
    //|null|Michael|
    //|  30|   Andy|
    //|  19| Justin|

    sqlDF.write.bucketBy(42, "name").sortBy("salary")
      .saveAsTable("people_bucketed3")

    val sqlDF2 = spark.sql("select * from people_bucketed3")
    sqlDF2.show

    spark.stop()
  }
}

// value write is not a member of Unit