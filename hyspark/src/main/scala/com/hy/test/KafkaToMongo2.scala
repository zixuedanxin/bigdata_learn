package com.hy.test
import com.mongodb.spark.MongoSpark
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.{BsonArray, BsonDocument, Document}

object KafkaToMongo2 {
  def main(args: Array[String]): Unit = {
    //创建streamingContext
    val conf=new SparkConf().setMaster("local[*]")
      .setAppName("SparkStreamKaflaWordCount Demo")
      .set("spark.mongodb.output.uri", "mongodb://usertest:123456@10.12.5.35:27017/applogs")
      .set("spark.mongodb.output.collection","hydd_log")
    val ssc=new StreamingContext(conf,Seconds(2))
    val topic=Array("hydd_log")
    val groupId="con-consumer-hydd_log22"
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
    )
    //创建DStream，返回接收到的输入数据
    val stream=KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent,Subscribe[String,String](topic,kafkaParam))
    //if(stream.>0l) {
    //每一个stream都是一个ConsumerRecord
    stream.foreachRDD({ rdd => {
      if (rdd.isEmpty()) {
      }
      else {
        rdd.map(s =>
          BsonDocument.parse(s.value().replaceAll("""\"\[""","[").replaceAll("""\]\"""","]")))

          .foreach(println)
          // MongoSpark.save(wordCounts)
        }

      }

    })
    // }
    ssc.start()
    ssc.awaitTermination()
  }
}
