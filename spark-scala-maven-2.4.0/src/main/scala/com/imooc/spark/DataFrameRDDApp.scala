package com.imooc.spark

import org.apache.spark.sql.SparkSession


/**
  * DataFrame和RDD的互操作
  * 采用反射的方式
  */

object DataFrameRDDApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    //RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("file:///f:/text/infos.txt")

    //z需要导入隐式转换
    import  spark.implicits._
    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt,line(1),line(2).toInt)).toDF()

    //第二种
    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age > 30").show()

    infoDF.filter(infoDF.col("age") > 30).show()

    spark.stop()

  }

  case class Info(id: Int, name: String, age:Int)

}
