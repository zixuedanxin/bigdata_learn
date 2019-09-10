package com.days

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

//��������case class
case class HotIP(user_id:Int,pv:Int)
//ֻ�õ�user_id��user_name
case class UserInfo(user_id:Int,username:String)

object BlackUserList {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)   
    
    //����һ��SparkContext��StreamingContext
    val conf = new SparkConf().setAppName("BlackUserList").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //ʹ���Ѿ����ڵ�SparkContext����StreamingContext
    val ssc = new StreamingContext(sc,Seconds(10))
    
    //����ʹ��Spark SQL�������ݣ�����SQLContext����
    val sqlContext = new SQLContext(ssc.sparkContext)
    import sqlContext.implicits._
    
    //�����û�����Ϣ
    val userInfo = sc.textFile("hdfs://hdp21:8020/input/userinfo.txt")
                     .map(_.split(","))
                     .map(x=> new UserInfo(x(0).toInt,x(1))).toDF
    userInfo.createOrReplaceTempView("userinfo")
    
    //��Kafka�н��յ����־�������û���PV
    val topics = Map("mytopic"->1)
    
//    //����Kafka����������ֻ��ʹ�û���Receiver��ʽ
//    val kafkaStream = KafkaUtils.createStream(ssc, "192.168.157.21:2181", "mygroup", topics)
//
//    //��־��1,201.105.101.102,http://mystore.jsp/?productid=1,2017020020,1,1
//    val logRDD = kafkaStream.map(_._2)
//
//    //ͳ���û���PV�����ڲ���                                                                           ÿ���û���ID����һ����
//    val hot_user_id = logRDD.map(_.split(",")).map(x=>(x(0),1))       //���ڵĳ���           ��������
//                            .reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(30),Seconds(10))
//
//    //�����쳣���û���Ϣ�����磺����Ƶ�ʴ���5�ε��û�ID
//    val result = hot_user_id.filter(x=> x._2 > 5)  //------> �õ����ʱ���ڵĺ�����
//
//    //��ѯ�������û�����Ϣ
//    result.foreachRDD( rdd =>{
//        //��ÿ���������û�ע���һ����
//      val hotUserTable = rdd.map(x=> new HotIP(x._1.toInt,x._2)).toDF
//      hotUserTable.createOrReplaceTempView("hotip")
//
//      //�����û�����ѯ����������Ϣ
//      sqlContext.sql("select userinfo.user_id,userinfo.username,hotip.pv from userinfo,hotip where userinfo.user_id=hotip.user_id").show()
//    })
//
    ssc.start()
    ssc.awaitTermination()
  }
}



















