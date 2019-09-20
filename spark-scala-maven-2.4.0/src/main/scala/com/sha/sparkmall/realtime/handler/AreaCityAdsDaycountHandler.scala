package com.sha.saprkmall.realtime.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.sha.saprkmall.realtime.bean.AdsLog
import com.sha.sparkmall.common.util.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * @author shamin
  * @create 2019-04-13 10:04 
  */
object AreaCityAdsDaycountHandler {
  //需求六  计算每天每个地区每个城市每个广告的点击量
  def handle(sparkContext: SparkContext, filteredBlackListDStream: DStream[AdsLog]) = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    sparkContext.setCheckpointDir("./checkPoint")
    val areaCityAdsDay: DStream[(String, Long)] = filteredBlackListDStream.map { adsLog =>
      //key :   area:city:ads:date   value : area:city:ads:date count
      val redisKey: String = adsLog.area + ":" + adsLog.city + ":" + adsLog.adsId + ":" + formatter.format(new Date(adsLog.ts))
      (redisKey, 1L)
    }

    val areaCityAdsDayCount: DStream[(String, Long)] = areaCityAdsDay.updateStateByKey { (value: Seq[Long], totalOption: Option[Long]) =>
      val sum: Long = value.sum
      val total: Long = totalOption.getOrElse(0L) + sum
      Option(total)
    }
    //将结果写入到redis中
    areaCityAdsDayCount.foreachRDD { rdd =>
      rdd.foreachPartition { areaCityAdsCountItr =>
        val redisKey = "area_city_ads_daycount"
        val jedisClient: Jedis = RedisUtil.getJedisClient
        val areaCityAdsCountMap: Map[String, String] = areaCityAdsCountItr.map {
          case (key, count) => (key, count.toString)
        }.toMap
        import scala.collection.JavaConversions._
        if (areaCityAdsCountMap != null && areaCityAdsCountMap.size > 0) {
          jedisClient.hmset(redisKey, areaCityAdsCountMap)
        }
        jedisClient.close()
      }
    }
    areaCityAdsDayCount
  }
}
