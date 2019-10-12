package com.sha.saprkmall.realtime.handler

import com.sha.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import scala.util.parsing.json.{JSONFormat, JSONObject}


/**
  * @author shamin
  * @create 2019-04-13 10:48 
  */
object AreaTop3AdsCountHandler {
  //  需求 七
  //    每天各地区 top3 热门广告
  //  数据来源 根据 需求六  得到    areaCityAdsDaycountDstream: DStream[(areaCityAdsDay  ：String, count Long)]
  //  //调整结构把 city去掉 重新组成key value
  //  areaCityAdsDaycountDstream.map{(areaCityAdsDay,count)=>(areaAdsDay,count)}
  //  //利用reducebykey的 新的汇总值
  //  areaCityAdsDaycountDstream: DStream[(areaAdsDay  ：String, count Long)]
  //  =>map >  DStream[(datekey,(area,(ads,count)))]
  //  =>groupbykey=>DStream[(datekey,iterable[(area,(ads,count))])]
  //  =>.map
  //  {
  //    iterable[(area,(ads,count)].groupby=>
  //
  //    Map[area ,iterable[(area,(ads,count)]]
  //      .map =>iterable[(ads,count)]
  //    // 排序  截取前三
  //    iterable[(ads,count)].tolist.sortwith .take(3)
  //  }
  //  DStream[datekey ,Map[area,Map[ads,count]]]
  //  hmset (datekey ,Map[area,Map[ads,count]]  )    count 是什么count    count =>把 每天各地区各城市各广告的点击流量  去掉城市 聚合一次可得

  //  hmset (datekey ,Map[area,JSON]  )       map=>  json
  //  hmset (key ,Map[String,String]  )
  def handle(areaCityAdsDayDStream: DStream[(String, Long)]) = {
    //调整结构把 city去掉 重新组成key value
    //DStream[(areaAdsDay  ：String, count Long)]
    //得到每天每个地区每个广告的点击量

    val areaAdsDaycountDstream: DStream[(String, Long)] = areaCityAdsDayDStream.map { case (areaCityAdsDay, count) =>
      val areaCityAdsDayArray: Array[String] = areaCityAdsDay.split(":")
      val area = areaCityAdsDayArray(0)
      val ads = areaCityAdsDayArray(2)
      val date = areaCityAdsDayArray(3)
      (area + ":" + ads + ":" + date, count)
    }.reduceByKey(_ + _)
    //每天  对应的  每个地区每个广告的点击量
    val areaAdsDaycountByDate: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsDaycountDstream.map { case (areaAdsDay, count) =>
      val areaAdsDayArray: Array[String] = areaAdsDay.split(":")
      val area = areaAdsDayArray(0)
      val ads = areaAdsDayArray(1)
      val date = areaAdsDayArray(2)
      val dateKey = "area_ads_date:" + date
      (dateKey, (area, (ads, count)))
    }.groupByKey()

    //DStream[datekey ,Map[area,Map[ads,count]]]
    val top3AreaAdsDateCount: DStream[(String, Map[String, Map[String, Long]])] = areaAdsDaycountByDate.map { case (dateKey, areaAdsCount) =>
      //根据area进行分组  Map[area ,iterable[(area,(ads,count)]]
      val areaAdscountByArea: Map[String, Iterable[(String, (String, Long))]] = areaAdsCount.groupBy(_._1)
      //求每个分组里的前3  Map[area,Map[ads,count]]
      val areaAdsCountSortedMap: Map[String, Map[String, Long]] = areaAdscountByArea.map { case (area, adsCountIte) =>
        val adsCountSorted: Map[String, Long] = adsCountIte.map { case (area, adsCount) => adsCount }.toList.sortWith(_._2 > _._2).take(3).toMap
        (area, adsCountSorted)
      }
      (dateKey, areaAdsCountSortedMap)
    }
    top3AreaAdsDateCount.foreachRDD { rdd =>
      rdd.foreachPartition { rdd2 =>
        val jedisClient: Jedis = RedisUtil.getJedisClient
        rdd2.foreach { case (datekey, areaAdsCountTop3Map) =>
          //将datekey ,Map[area,Map[ads,count]]  转化为  datekey ,Map[area,JSON]
          val areaAdsCountMapJson: Map[String, String] = areaAdsCountTop3Map.map { case (area, adsCount) =>
            val adsCountJson: JSONObject = JSONObject(adsCount)
            (area, adsCountJson.toString(JSONFormat.defaultFormatter))
          }

          import scala.collection.JavaConversions._
          jedisClient.hmset(datekey, areaAdsCountMapJson)
        }
        jedisClient.close()
      }


    }


  }
}
