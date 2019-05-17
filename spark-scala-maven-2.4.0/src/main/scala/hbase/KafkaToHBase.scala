package com.hy.spark

import java.io.FileInputStream
import java.util.Properties

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.client._
import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.{MultiTableOutputFormat, TableOutputFormat} //MultiTableOutputFormat
//import com.mongodb.spark.MongoSpark
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.bson.{BsonArray, BsonDocument, Document}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object KafkaToHBase {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    //创建streamingContext
    val properties = new Properties()
    val configFile = System.getProperty("user.dir") + "/config.properties"
    properties.load(new FileInputStream(configFile))
    val topics = properties.get("topics").toString
    val batchSleepSec = properties.get("batchSleepSec").toString
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("SparkStreamKafkaToMongodb")

    val ssc = new StreamingContext(conf, Seconds(batchSleepSec.toInt))
    ssc.sparkContext.setLogLevel("WARN")
    val sc=ssc.sparkContext
    val topic = topics.split("\\W") // Array("hydd_log","testlog","df") 配置消费主题
    val groupId = properties.get("group_id").toString + "hbase"
    val ColFamily = "cf"
    // 不能随便改topics.replace(",","_")+"-consumer"
    //消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> properties.get("bootstrap.servers").toString,
      "group.id" -> groupId,
      "max.poll.records" -> "50",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "security.protocol" -> "SASL_PLAINTEXT",
      "sasl.mechanism" -> "PLAIN",
      // "sasl.kerberos.service.name" -> "kafka", 可以不配置
      "sasl.jaas.config" -> properties.get("sasl.jaas.config").toString,
      "auto.offset.reset" -> "earliest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","localhost")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "applogs")
    val job = Job.getInstance(sc.hadoopConfiguration) // 过时用法 ： new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]]) // TableOutputFormat  存在两个mapreduce mapre
    //job.setOutputFormatClass(classOf[MultiTableOutputFormat])           // 插入 不同的表
    //创建DStream，返回接收到的输入数据
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topic, kafkaParam))

    //每一个stream都是一个ConsumerRecord
    def parse_json(json_strs: String): BsonDocument = {
      try {
        return BsonDocument.parse(json_strs
          .replaceAll("""\"\[""", "[")
          .replaceAll("""\]\"""", "]"))
      } catch {
        case e: Exception => {
          logger.error("Exception: json :" + json_strs + " e:" + e)
          return BsonDocument.parse("{\"errors\":[\"errors\"]}")
        }
      }
    }
    // 将json
    def json_to_put(json: Document):  (ImmutableBytesWritable, Put) = {
      val crt_dtm=json.get("crt_dtm").toString
      val topic=json.getOrDefault("topic","空").toString
      val user_id=json.getOrDefault("user_id","空").toString
      val uuid=json.getOrDefault("uuid",json.getOrDefault("user_ip","空")).toString
      //val user_ip=json.getOrDefault("user_ip","空").toString
      val put = new Put(Bytes.toBytes(crt_dtm+"^"+topic+"^"+user_id+"^"+uuid))
      def putAdd(col_nm:String){
        val col_value = json.get(col_nm).toString
        put.addColumn(Bytes.toBytes(ColFamily), Bytes.toBytes(col_nm), Bytes.toBytes(col_value))
      }
      try {
          val keys = json.keySet
          keys.toArray.map(x => putAdd(x.toString) )
//          val iterator_redis = keys.iterator
//          while (iterator_redis.hasNext) {
//            val col_nm = iterator_redis.next()//.toString
//            val col_value = json.get(col_nm).toString
//            put.addColumn(Bytes.toBytes(ColFamily), Bytes.toBytes(col_nm), Bytes.toBytes(col_value))
//            //result_list.append((new ImmutableBytesWritable(Bytes.toBytes(tablename)), put)) // 多表
//            //result_list.append((new ImmutableBytesWritable(), put)) // 单表
//            }
          return (new ImmutableBytesWritable(), put) //result_list.iterator
        } catch {
            case e: Exception => {
              logger.error("Exception: json :" + json.toString() + " \t e:" + e.toString)
              put.addColumn(Bytes.toBytes(ColFamily), Bytes.toBytes("raw_json"), Bytes.toBytes(json.toJson()))
              return (new ImmutableBytesWritable(), put)
            }
        }
    }

    stream.foreachRDD({ rdd => {
      if (rdd.isEmpty()) {
        // 为空时不处理
      } else {
        // replace 是对字符串格式的json做特殊处理，减少解析错误
        val wordCounts = rdd.map(s =>
          (s.topic(), parse_json(s.value()))
        ).map(docs => { // 获取 json数组
                if (docs._2.containsKey("data")) {
                  (docs._2.getArray("data"), docs._1)
                }
                else {
                  val doc = new BsonArray()
                  (doc, "")
                }
        })
          .flatMap(doc => {
            doc._1.toArray().map(x => (x.toString, doc._2))
          }) // 数组变化
          .map(doc => {
                val dict = Document.parse(doc._1.toString)
                dict.put("topic", doc._2)
                if (dict.containsKey("crt_dtm")) { // 添加时间锉
                  // doc.put("crt_dtm", doc.get("crt_dtm").toLong)
                } else {
                  dict.put("crt_dtm", System.currentTimeMillis())
                }
                dict.put("etl_dtm", System.currentTimeMillis())
                json_to_put(dict)
        })

        wordCounts.saveAsNewAPIHadoopDataset(job.getConfiguration)
        rdd.foreach(s => logger.warn("Process topic:" + s.topic() + " Partition:" + s.partition().toString + " offset:" + s.offset().toString))
      }
    }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
