package com.hy

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark.MongoSpark
import org.bson.BasicBSONObject
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object SparkMongodbReader {
  def main(args: Array[String]): Unit = {
    //创建streamingContext
    var conf=new SparkConf().setMaster("local[*]")
      .setAppName("SparkStreamKaflaWordCount Demo")
      .set("spark.mongodb.input.uri", "mongodb://usertest:123456@10.12.5.35:27017/applogs")
      .set("spark.mongodb.input.collection","testlog")
      .set("spark.mongodb.output.uri", "mongodb://usertest:123456@10.12.5.35:27017/applogs")
      .set("spark.mongodb.output.collection","testlog2");
    // var ssc=new StreamingContext(conf,Seconds(2));
    var sc=new SparkContext(conf)
    //创建topic
    //var topic=Map{"test" -> 1}
    var topic=Array("test");
    //指定zookeeper
    //创建消费者组
    val  rdd=MongoSpark.load(sc)
   // MongoSpark.save(rdd) // 读取后直接插入
    // rdd.map(m=>{MongoSpark.
     // println(rdd.first().toJson)
    //创建DStream，返回接收到的输入数据
    // var stream=KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent,Subscribe[String,String](topic,kafkaParam))
    //每一个stream都是一个ConsumerRecord
    //stream.map(s =>(s.key(),s.value())).print();
    //ssc.start();
    // ssc.awaitTermination();
  }
}