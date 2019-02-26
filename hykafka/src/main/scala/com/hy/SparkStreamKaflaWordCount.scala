package com.hy
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
object SparkStreamKaflaWordCount {
  def main(args: Array[String]): Unit = {
    //创建streamingContext
    var conf=new SparkConf().setMaster("local[*]")
      .setAppName("SparkStreamKaflaWordCount Demo");
    var ssc=new StreamingContext(conf,Seconds(2));
    //创建topic
    //var topic=Map{"test" -> 1}
    var topic=Array("test");
    //指定zookeeper
    //创建消费者组
    var groupId="con-consumer-group"
    val uriStr = "mongodb://dps:dps1234@broker1:27017/applogs.test1"
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
    stream.map(s =>(s.key(),s.value())).print();
    ssc.start();
    ssc.awaitTermination();
  }
}