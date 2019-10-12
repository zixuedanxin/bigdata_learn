package com.xuzh.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * @author zhangjin
  * @create 2018-06-14 17:51
  */
object Jtools {


  private val poolConfig = new GenericObjectPoolConfig()
  poolConfig.setMaxTotal(1000) //支持最大的连接数  同时最多有多少个连接
  poolConfig.setMaxIdle(5) //支持最大的空闲链接
  private val jedisPool: JedisPool = new JedisPool(poolConfig, "hdp1", 6379)

  def getJedisPool: Jedis = {
    //从池子中返回一个链接
    val jedis: Jedis = jedisPool.getResource
//    jedis.select(8)
    jedis
  }

  def main(args: Array[String]): Unit = {


  }

}
