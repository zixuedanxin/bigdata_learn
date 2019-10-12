package com.days

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.lang.Double

//����һ��case class����������
//ָ��DataFrame��schema
case class OrderInfo(year:Int,amount:Double)

object AnnualTotal {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AnnualTotal").setMaster("local")
    
    //����SparkSession��Ҳ����ֱ�Ӵ���SQLContext
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    //��HDFS�϶�������
    val myData = sc.textFile("hdfs://hdp21:8020/input/sh/sales").map(
        //13,1660,1998-01-10,3,999,1,1232.16
       line => {
         val words = line.split(",")
         
         //ȡ�� ��ݺͽ��
         (Integer.parseInt(words(2).substring(0,4)),Double.parseDouble(words(6)))
     }).map(d=>OrderInfo(d._1,d._2)).toDF()
     
     //myData.printSchema()
     
     //������ͼ
     myData.createTempView("orders")
     
     //ִ�в�ѯ  1��SparkSession.sql()   2��sqlContext.sql()
     sqlContext.sql("select year,count(amount),sum(amount) from orders group by year").show()
     
     sc.stop()
    
  }
}














