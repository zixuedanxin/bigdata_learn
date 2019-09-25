package cn.huage.core

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author zhangjin
  * @create 2018-06-28 20:08
  */
object ReadParquet {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName)
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()


    val df: DataFrame = session.sqlContext.read.parquet("/Users/zhangjin/myCode/learn/spark-dmp/dmp.parquet")
    df.createTempView("logs")
    val df2: DataFrame = df.sqlContext.sql("select * from logs")
    df2.collect().foreach(println)
  }

}
