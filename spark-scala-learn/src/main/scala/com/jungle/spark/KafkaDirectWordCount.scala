package com.imooc.spark


import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
// import kafka.serializer.StringDecoder
// import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * Spark Streaming对接Kafka的方式二
  */
object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: KafkaDirectWordCount <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf() //.setAppName("KafkaReceiverWordCount")
    //.setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    // TODO... Spark Streaming如何对接Kafka
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
    //创建DStream，返回接收到的输入数据
    val topics2 = Array("test")
    // subscribe	A comma-separated list of topics	用于指定要消费的 topic
    val messages = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics2, kafkaParam))
    //val messages = KafkaUtils.cr[String,String,StringDecoder,StringDecoder](


    // TODO... 自己去测试为什么要取第二个
    messages.map(_.value()).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
