package com.days

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.rdd.RDD



//ʹ��Spark SQL������ʽ����
//��־����: 1,201.105.101.102,http://mystore.jsp/?productid=1,2017020029,2,1
case class LogInfo(user_id:String,user_ip:String,url:String,click_time:String,action_type:String,area_id:String)

//object HotIP {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
//
//    //����һ��StreamingContext
//    val conf = new SparkConf().setAppName("HotIP").setMaster("local[2]")
//    val ssc = new StreamingContext(conf,Seconds(10))
//
//    //����ʹ��Spark SQL�������ݣ�����SQLContext����
//    val sqlContext = new SQLContext(ssc.sparkContext)
//    import sqlContext.implicits._
//
//    //ָ��Kafka��Topic����ʾ�����topic�У�ÿ�ν���һ������
//    val topic = Map("mytopic"->1)
//
//    //����DStream��������
////    val kafkaStream = KafkaUtils.createStream(ssc,"192.168.157.21:2181","mygroup",topic)
////    //��kafka�У����յ���������<key value>   key----> null��ֵ
////    val logRDD = kafkaStream.map(_._2)  //ȡ��value
////
////    //��־��1,201.105.101.102,http://mystore.jsp/?productid=1,2017020029,2,1
////    logRDD.foreachRDD((rdd:RDD[String]) =>{
////      //����DataFrame
////      val result = rdd.map(_.split(",")).map(x=> new LogInfo(x(0),x(1),x(2),x(3),x(4),x(5))).toDF()
////
////      //������ͼ
////      result.createOrReplaceTempView("clicklog")
////      //ִ��SQL
////      sqlContext.sql("select user_ip as IP, count(user_ip) as PV from clicklog group by user_ip").show
////    })
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}



















