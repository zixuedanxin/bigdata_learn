package com.xuzh.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 使用Spark完成我们的数据清洗操作（静态统计，做一些清洗操作）
 */
object SparkStatCleanJob {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.parquet.compression.codec","gzip")
      .master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file:///Users/lipan/app/data/test_data/access.log")

//    accessRDD.take(10).foreach(println)

    //将RDD ==> DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

//    accessDF.printSchema()
//    accessDF.show(false)

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .partitionBy("day").save("file:///Users/lipan/app/data/test_data/out_data/clean2")

    spark.stop
  }
}
