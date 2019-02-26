package com.hy

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.api.java.JavaInputDStream
import org.apache.kafka.clients.consumer.ConsumerRecord

/*
* spark消费kafka例子
*
* 2018/5/13
*/
object SparkKafkaTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark_kafka").master("local[*]").getOrCreate()
    val batchDuration = Seconds(20) //时间单位为秒
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    //http://spark.apache.org/docs/2.1.2/streaming-kafka-0-10-integration.html
    // offset保存路径
//    val checkpointPath = "D:\\hadoop\\checkpoint\\kafka-direct"
//
//    val conf = new SparkConf()
//      .setAppName("ScalaKafkaStream")
//      .setMaster("local[2]")
//
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("WARN")
//
//    val ssc = new StreamingContext(sc, Seconds(5))
    //ssc.checkpoint("/home/xzh/data/checkpoint") //checkpoint(checkpointPath)

    val bootstrapServers = "broker1:9092,broker2:9092"
    val groupId = "kafka-test-grou"
    val topicName = "app_log"
    val maxPoll = 500

    val kafkaParams = Map(
      "bootstrap.servers" -> bootstrapServers,
      "group.id"-> groupId,
      "max.poll.records"-> maxPoll.toString,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest",
      "security.protocol"-> "SASL_PLAINTEXT",
      "sasl.mechanism"-> "PLAIN",
      "sasl.kerberos.service.name" -> "kafka",
      "sasl.jaas.config"-> "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"mooc\" password=\"moocpswd\";"
    )

    val stream  = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParams))
//    JavaInputDStream<ConsumerRecord<String, String>> stream=KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParams))
    println("开始的-------------------------------------")

    // print(kafkaTopicDS)
    // println(kafkaTopicDS.count())
   // kafkaTopicDS.map(_.value)
//      .flatMap(_.split(" "))
//      .map(x => (x, 1L))
//      .reduceByKey(_ + _)
//      .transform(data => {
//        val sortData = data.sortBy(_._2, false)
//        sortData
//      })
     // .print()
    stream.map(record => (record.key, record.value)).print()
    println("-----------------------------++++++++++++++end")
    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }


//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder().appName("spark_kafka").master("local[*]").getOrCreate()
//    val batchDuration = Seconds(5) //时间单位为秒
//    val streamContext = new StreamingContext(spark.sparkContext, batchDuration)
//    streamContext.checkpoint("/Users/eric/SparkWorkspace/checkpoint")
//    val topics = Array("behaviors").toSet
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
//    val stream = KafkaUtils.createDirectStream[String, String](streamContext, kafkaParams, topics)
//    stream.foreachRDD(rdd => {
//      rdd.foreach(line => {
//        println("key=" + line._1 + "  value=" + line._2)
//      })
//    })
//    streamContext.start()  //spark stream系统启动
//    streamContext.awaitTermination() //
//  }
}
