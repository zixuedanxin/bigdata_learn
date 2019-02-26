package com.hy.spark
import java.io.FileInputStream
import java.util.Properties
import com.mongodb.spark.MongoSpark
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.bson.{BsonArray, BsonDocument, Document}

object KafkaToMongo {
  def main(args: Array[String]): Unit = {
    //创建streamingContext
    val properties = new Properties()
    val configFile = System.getProperty("user.dir") + "/config.properties"
    properties.load(new FileInputStream(configFile))
    val topics = properties.get("topics").toString
    val mongodb_url=properties.get("spark.mongodb.output.uri").toString
    val batchSleepSec=properties.get("batchSleepSec").toString
    val mongodb_db=properties.get("spark.mongodb.output.database").toString
    val mongodb_coll=properties.get("spark.mongodb.output.collection").toString
    val conf=new SparkConf().setMaster("local[*]")
      .setAppName("SparkStreamKaflaToMongodb")
      .set("spark.mongodb.output.uri",mongodb_url)
      .set("spark.mongodb.output.database",mongodb_db)
      .set("spark.mongodb.output.collection",mongodb_coll)
    val ssc=new StreamingContext(conf,Seconds(batchSleepSec.toInt))
    val topic=  topics.split("\\W") // Array("hydd_log","testlog","df") 配置消费主题
    val groupId=topics.replace(",","_")+"-consumer"
    //消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" ->  properties.get("bootstrap.servers").toString,
      "group.id"-> groupId,
      "max.poll.records"-> "50",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "security.protocol"-> "SASL_PLAINTEXT",
      "sasl.mechanism"-> "PLAIN",
      // "sasl.kerberos.service.name" -> "kafka", 可以不配置
      "sasl.jaas.config"-> properties.get("sasl.jaas.config").toString,
      "auto.offset.reset" -> "earliest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    //创建DStream，返回接收到的输入数据
    val stream=KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent,Subscribe[String,String](topic,kafkaParam))
      //每一个stream都是一个ConsumerRecord
      stream.foreachRDD({ rdd => {
        if (rdd.isEmpty()) {
          // 为空时不处理
           } else {
          // replace 是对字符串格式的json做特殊处理，减少解析错误
          val wordCounts =rdd.map(s => (s.topic(), BsonDocument.parse(s.value().replaceAll("""\"\[""","[").replaceAll("""\]\"""","]"))))
                          .map(docs=>{  // 获取 json数组
                              if(docs._2.containsKey("data")) {
                                (docs._2.getArray("data"),docs._1)
                              }else{
                                val doc=new BsonArray()
                                (doc,"")
                              }
                            })
                          .flatMap(doc=> {doc._1.toArray().map(x=>(x.toString,doc._2))}) // 数组变化
                          .map(doc=>{
                              val dict=Document.parse(doc._1.toString)
                              dict.put("topic",doc._2)
                              if(dict.containsKey("crt_dtm")) { // 添加时间锉
                                  // doc.put("crt_dtm", doc.get("crt_dtm").toLong)
                                }else{
                                  dict.put("crt_dtm" , System.currentTimeMillis())
                                }
                              dict
                            })
          //val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(WriteConfig(ssc)))
          // , WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority")
          MongoSpark.save(wordCounts) // 存储到monogdb
        }
      }
      })
    ssc.start()
    ssc.awaitTermination()
  }
}
