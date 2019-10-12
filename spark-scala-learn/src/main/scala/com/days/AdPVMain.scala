package com.days

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

//����case class
case class AreaInfo(area_id:Int,area_name:String)
case class AdLogInfo(userid:Int,ip:String,clickTime:String,url:String,area_id:Int)


object AdPVMain {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)   
    
    //����SparkSession����
    val spark = SparkSession.builder().master("local").appName("AdPVMain").getOrCreate()
    import spark.sqlContext.implicits._
    
    //����������
    val areaInfoDF = spark.sparkContext.textFile("hdfs://hdp21:8020/input/project07/areainfo.txt")
                          .map(_.split(",")).map(x=>new AreaInfo(x(0).toInt,x(1))).toDF
    areaInfoDF.createOrReplaceTempView("areainfo")
    
    //�����������־��
    val adClickInfoDF = spark.sparkContext.textFile("hdfs://hdp21:8020/flume/20180710/events-.1531169622734")
                             .map(_.split(",")).map(x=>new AdLogInfo(x(0).toInt,x(1),x(2),x(3),x(4).toInt)).toDF
    adClickInfoDF.createOrReplaceTempView("adclickinfo")
    
    //����SQL
    var sql = "select adclickinfo.url,areainfo.area_name,adclickinfo.clicktime,count(adclickinfo.clicktime) "
    sql = sql + "from areainfo,adclickinfo "
    sql = sql + "where areainfo.area_id=adclickinfo.area_id "
    sql = sql + "group by adclickinfo.url,areainfo.area_name,adclickinfo.clicktime"
    
    //ֱ���������Ļ
    spark.sql(sql).show
    
    spark.stop()
  }
}