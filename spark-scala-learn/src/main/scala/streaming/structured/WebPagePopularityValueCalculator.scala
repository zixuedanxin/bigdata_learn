package streaming.structured

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.kafka.common.serialization.StringDeserializer

object WebPagePopularityValueCalculator {

  private val checkpointDir = "popularity-data-checkpoint"
  private val msgConsumerGroup = "user-behavior-topic-message-consumer-group"
  val properties = new Properties()
  val configFile = System.getProperty("user.dir") + "/config.properties"
  properties.load(new FileInputStream(configFile))
  val topics = properties.get("topics").toString

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage:WebPagePopularityValueCalculator zkserver1:2181, zkserver2: 2181, zkserver3: 2181 consumeMsgDataTimeInterval (secs) ")
      System.exit(1)
    }

    val Array(zkServers, processingInterval) = args
    val conf = new SparkConf().setAppName("Web Page Popularity Value Calculator")

    val ssc = new StreamingContext(conf, Seconds(processingInterval.toInt))
    val topic=  Array("hydd_log","testlog","df") //配置消费主题
    val groupId=properties.get("group_id").toString// 不能随便改topics.replace(",","_")+"-consumer"
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
    //using updateStateByKey asks for enabling checkpoint
    ssc.checkpoint(checkpointDir)
    val kafkaStream=KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent,Subscribe[String,String](topic,kafkaParam))
//    val kafkaStream = KafkaUtils.createStream(
//      //Spark streaming context
//      ssc,
//      //zookeeper quorum. e.g zkserver1:2181,zkserver2:2181,...
//      zkServers,
//      //kafka message consumer group ID
//      msgConsumerGroup,
//      //Map of (topic_name -> numPartitions) to consume. Each partition is consumed in its own thread
//      Map("user-behavior-topic" -> 3))
    val msgDataRDD = kafkaStream.map(_.value())

    //for debug use only
    //println("Coming data in this interval...")
    //msgDataRDD.print()
    // e.g page37|5|1.5119122|-1
    val popularityData = msgDataRDD.map { msgLine => {
      val dataArr: Array[String] = msgLine.split("\\|")
      val pageID = dataArr(0)
      //calculate the popularity value
      val popValue: Double = dataArr(1).toFloat * 0.8 + dataArr(2).toFloat * 0.8 + dataArr(3).toFloat * 1
      (pageID, popValue)
    }
    }

    //sum the previous popularity value and current value
    //定义一个匿名函数去把网页热度上一次的计算结果值和新计算的值相加，得到最新的热度值。

    val updatePopularityValue = (iterator: Iterator[(String, Seq[Double], Option[Double])]) => {
      iterator.flatMap(t => {
        val newValue: Double = t._2.sum
        val stateValue: Double = t._3.getOrElse(0);
        Some(newValue + stateValue)
      }.map(sumedValue => (t._1, sumedValue)))
    }

    val initialRDD = ssc.sparkContext.parallelize(List(("page1", 0.00)))

    //调用 updateStateByKey 原语并传入上面定义的匿名函数更新网页热度值。
    val stateDStream = popularityData.updateStateByKey[Double](updatePopularityValue,
      new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)

    //set the checkpoint interval to avoid too frequently data checkpoint which may
    //may significantly reduce operation throughput
    stateDStream.checkpoint(Duration(8 * processingInterval.toInt * 1000))

//after calculation, we need to sort the result and only show the top 10 hot pages
//最后得到最新结果后，须要对结果进行排序。最后打印热度值最高的 10 个网页。
    stateDStream.foreachRDD { rdd => {
      val sortedData = rdd.map { case (k, v) => (v, k) }.sortByKey(false)
      val topKData = sortedData.take(10).map { case (v, k) => (k, v) }
      topKData.foreach(x => {
        println(x)
      })
    }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

