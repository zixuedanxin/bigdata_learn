package com.hy

import com.mongodb.spark.MongoSpark
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.bson.BasicBSONObject
import org.bson.Document

object SparkKafkaToMongo {
  def main(args: Array[String]): Unit = {
    //创建streamingContext
    var conf=new SparkConf().setMaster("local[*]")
      .setAppName("SparkStreamKaflaWordCount Demo")
      .set("spark.mongodb.output.uri", "mongodb://usertest:123456@10.12.5.35:27017/applogs")
      .set("spark.mongodb.output.collection","testlog2");
    var ssc=new StreamingContext(conf,Seconds(2));
    var topic=Array("app_log");
    var groupId="con-consumer-group2"
    //消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" ->  "broker1:9092,broker2:9092",
      "group.id"-> groupId,
      "max.poll.records"-> "50",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "security.protocol"-> "SASL_PLAINTEXT",
      "sasl.mechanism"-> "PLAIN",
      "sasl.kerberos.service.name" -> "kafka",
      "sasl.jaas.config"-> "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"mooc\" password=\"moocpswd\";",
      "auto.offset.reset" -> "earliest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (true: java.lang.Boolean)
    );
    //创建DStream，返回接收到的输入数据
    var stream=KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent,Subscribe[String,String](topic,kafkaParam))
    //每一个stream都是一个ConsumerRecord
    //var rdd=stream.map(s =>(s.key(),s.value())).map//print();
    //var rdd=stream.map(s =>Document.parse(s.value())).map(doc => doc.saveToMongoDB());
//    stream.foreachRDD({ rdd =>
//      val wordCounts = rdd.map(s =>Document.parse(s.value()))
//      MongoSpark.save(wordCounts)
//    })
    stream.foreachRDD({ rdd =>
      val wordCounts = rdd.map(s =>Document.parse(s.value()))
                        .map(doc => {
                              if(doc.containsKey("crt_dtm")) {
                                doc.put("crt_dtm", doc.getString("crt_dtm").toLong)
                              }
                             doc
                          })
      MongoSpark.save(wordCounts)
    })
    // Document.parse(s"{test: $i}")
    // MongoSpark.save(rdd); // 读取后直接插入
    ssc.start();
    ssc.awaitTermination();
  }
}