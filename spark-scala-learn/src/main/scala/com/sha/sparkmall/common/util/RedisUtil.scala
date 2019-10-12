package com.sha.sparkmall.common.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @author shamin
  * @create 2019-04-12 20:41 
  */
object RedisUtil {

  var jedisPool:JedisPool=null

  def getJedisClient: Jedis = {
    if(jedisPool==null){
      println("开辟一个连接池")
      val properties = PropertiesUtil.load("config.properties")
      val host = properties.getProperty("redis.host")
      val port = properties.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100)  //最大连接数
      jedisPoolConfig.setMaxIdle(20)   //最大空闲
      jedisPoolConfig.setMinIdle(20)     //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500)//忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      jedisPool=new JedisPool(jedisPoolConfig,host,port.toInt)
    }
    println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
    println("获得一个连接")
    jedisPool.getResource
  }
}

