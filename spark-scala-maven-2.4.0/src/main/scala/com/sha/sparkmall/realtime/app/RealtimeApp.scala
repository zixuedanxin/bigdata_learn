package com.sha.saprkmall.realtime.app

import java.util.Properties

import com.sha.saprkmall.realtime.bean.AdsLog
import com.sha.sparkmall.common.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.sha.saprkmall.realtime.handler.{AreaCityAdsDaycountHandler, AreaTop3AdsCountHandler, BlackListHandler, LastHourAdsCountHandler}
/**
  * @author shamin
  * @create 2019-04-12 20:24
  */
object RealtimeApp {
  def main(args: Array[String]): Unit = {
    //获取ssc
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealtimeApp")
    val ssc : StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic: String = properties.getProperty("topic")
    //创建DStream
    val adsLogInputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,ssc)

    val adsLogDStream: DStream[AdsLog] = adsLogInputDStream.map { rdd =>

      val adsLogArray: Array[String] = rdd.value().split(" ")

      AdsLog(adsLogArray(0).toLong, adsLogArray(1), adsLogArray(2), adsLogArray(3).toLong, adsLogArray(4).toLong)

    }

    //过滤掉黑名单
    val filteredBlackListDStream: DStream[AdsLog] = BlackListHandler.filterBlackList(ssc,adsLogDStream)
    //需求五  更新日志信息
 //   BlackListHandler.updateUserAdsCount(filteredBlackListDStream)
    //测试消费kafka的数据成功
//   adsLogDStream.foreachRDD{
//     rdd =>
//       println(rdd.map(_.value()).collect().mkString(" "))
//   }

    //需求六
  //  val areaCityAdsDayDStream: DStream[(String, Long)] = AreaCityAdsDaycountHandler.handle(ssc.sparkContext,filteredBlackListDStream)

    //需求七
 //   AreaTop3AdsCountHandler.handle(areaCityAdsDayDStream)

    //需求八
    LastHourAdsCountHandler.handle(filteredBlackListDStream)
    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
