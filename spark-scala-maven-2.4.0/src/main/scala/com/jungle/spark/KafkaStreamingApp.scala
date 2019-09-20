package com.jungle.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming对接Kafka
  */
object KafkaStreamingApp {

  def main(args: Array[String]): Unit = {

    if(args.length != 4) {
      System.err.println("Usage: KafkaStreamingApp <zkQuorum> <group> <topics> <numThreads>")
    }

    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("KafkaReceiverWordCount")
      .setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaParam = Map(
      "bootstrap.servers" -> "",
      "group.id" -> "",
      "max.poll.records" -> "50",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "security.protocol" -> "SASL_PLAINTEXT",
      "sasl.mechanism" -> "PLAIN",
      // "sasl.kerberos.service.name" -> "kafka", 可以不配置
      "sasl.jaas.config" -> "",
      "auto.offset.reset" -> "earliest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    // TODO... Spark Streaming如何对接Kafka
    val topics2 = Array("test")
    // TODO... Spark Streaming如何对接Kafka
    val messages = KafkaUtils.createDirectStream(ssc,PreferConsistent,Subscribe[String,String](topics2,kafkaParam))//createStream(ssc, zkQuorum, group,topicMap)

    // TODO... 自己去测试为什么要取第二个
    messages.map(_.value()).count().print()

    ssc.start()
    ssc.awaitTermination()
  }
}
