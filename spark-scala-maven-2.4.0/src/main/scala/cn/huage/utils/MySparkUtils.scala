package cn.huage.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhangjin
  * @create 2018-06-06 09:24
  */
object MySparkUtils {


  def getLocalSparkContext(appName: String): SparkContext = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(appName)
    //spark.serializer=org.apache.spark.serializer.KryoSerialization
    conf.set("park.sql.parquet.compression.codec","snappy")

    val sc: SparkContext = new SparkContext(conf)

    sc
  }



  def getLocalSQLContext(appName: String) = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(appName)
    //
    val sc: SparkContext = new SparkContext(conf)

    val sqlSC = new SQLContext(sc)
    sqlSC
  }
}
