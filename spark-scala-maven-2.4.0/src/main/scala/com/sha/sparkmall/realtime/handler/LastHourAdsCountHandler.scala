package com.sha.saprkmall.realtime.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.sha.saprkmall.realtime.bean.AdsLog
import com.sha.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis


/**
  * @author shamin
  * @create 2019-04-14 16:25 
  */
object LastHourAdsCountHandler {

  //需求八  求每小时 每个广告 每分钟的点击量实时统计
  //保存到redis中的hash结构
  //key  last_hour_ads_click   field:    11      value:  {“8:30”:122, “8:31”:32, “8:32”:910, “8:33”:21 , ………
  //=>Map(adid,(8:20:count))
  //=>Map(adid_08_20,count)
  //=>Map(adid_08_20,1)
  def handle(filteredBlackListDStream: DStream[AdsLog]) = {
    val adsHourMinuteCountWindowsDStream: DStream[AdsLog] = filteredBlackListDStream.window(Minutes(60), Seconds(10))
    val adsHourMinuteCountDStream: DStream[(String, Long)] = adsHourMinuteCountWindowsDStream.map { adsLog =>
      val adsId: Long = adsLog.adsId
      val ts: Long = adsLog.ts
      val formatter = new SimpleDateFormat("HH:mm")
      val hourMinute: String = formatter.format(new Date(ts))
      val adsIdTSMapKey = adsId + "_" + hourMinute
      (adsIdTSMapKey, 1L)
    }.reduceByKey(_ + _)
    val HourMinuteCountbyAdsDStream: DStream[(String, Iterable[(String, Long)])] = adsHourMinuteCountDStream.map { case (adsHourMinute, count) =>
      val adsId: String = adsHourMinute.split("_")(0)
      val hourMinute: String = adsHourMinute.split("_")(1)
      (adsId, (hourMinute, count))
    }.groupByKey()
    val hourMinuteCountStringByAdsid: DStream[(String, String)] = HourMinuteCountbyAdsDStream.map { case (adsId, hourMinuteCountItr) =>
      //将hourMinuteConutItr转换为json字符串
      import org.json4s.JsonDSL._
      val hourMinuteCountString: String = JsonMethods.compact(JsonMethods.render(hourMinuteCountItr))
      (adsId, hourMinuteCountString)
    }
    hourMinuteCountStringByAdsid.foreachRDD { rdd =>
      rdd.foreachPartition { hourMinuteCountItr =>
        val jedis: Jedis = RedisUtil.getJedisClient
        val key = "last_hour_ads_click"
        import scala.collection.JavaConversions._

        if (hourMinuteCountItr.hasNext) {
          jedis.hmset(key, hourMinuteCountItr.toMap)
        }
        jedis.close()
      }
    }
  }
}
