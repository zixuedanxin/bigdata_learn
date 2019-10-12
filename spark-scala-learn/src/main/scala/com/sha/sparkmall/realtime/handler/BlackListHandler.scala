package com.sha.saprkmall.realtime.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.sha.saprkmall.realtime.bean.AdsLog
import com.sha.sparkmall.common.util.RedisUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis


/**
  * @author shamin
  * @create 2019-04-12 20:27
  */
object BlackListHandler {
  //需求五
  //广告黑名单实时统计 ,用户每天点  某个广告  的 次数
  //更新redis中的数据
  def updateUserAdsCount(adsLogDStream: DStream[AdsLog]): Unit = {
    //将数据写入到redis中
    adsLogDStream.foreachRDD { rdd =>
      rdd.foreachPartition { adsLogPartition =>
        //每个分区获取一次redis的连接。避免获取连接太频繁
        val jedisClient: Jedis = RedisUtil.getJedisClient
        val key = "user_adsid_date"
        val formatter = new SimpleDateFormat("yyyy-MM-dd")
        val blackListKey = "userid"
        adsLogPartition.foreach { adsLog =>
          //更新的redis的结构为hash
          //hash : key      field    value     user_adsid_date    user:adsid:date   count
          //adsLog : 1555074255607 华南 深圳 2 6


          val ts: Long = adsLog.ts
          val date: String = formatter.format(new Date(ts))
          val field: String = adsLog.uid + ":" + adsLog.adsId + ":" + date
          jedisClient.hincrBy(key, field, 1L)
          val countStr: String = jedisClient.hget(key, field)

          //如果点击次数大于等于100，那么就将该用户加入到黑名单中
          if (countStr.toLong >= 100) {
            jedisClient.sadd(blackListKey, adsLog.uid.toString)
          }
        }
        jedisClient.close()
      }
    }
  }

  //如果用户在黑名单中则不写入redis中
  def filterBlackList(ssc: StreamingContext, adsLogDStream: DStream[AdsLog])= {

    //1.获取黑名单中的用户
    //transform里面的代码会执行多次
    val filteredAdsLogDStream: DStream[AdsLog] = adsLogDStream.transform { adsLogRDD =>
      //调用多次jedis获取黑名单的用户
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //读取黑名单中的用户
      val useridList: util.Set[String] = jedisClient.smembers("userid")
      //关掉连接
      jedisClient.close()
      //要对每个RDD做过滤，那么将这个Array声明为广播变量
       val useridlistBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(useridList)

      //2.与当前用户作对比，如果为黑名单中的用户，则过滤(broadcast)掉
      val filteredAdsLog: RDD[AdsLog] = adsLogRDD.filter { adsLog =>
        !useridlistBC.value.contains(adsLog.uid)
      }
      filteredAdsLog
    }
    filteredAdsLogDStream
  }
}
