package com.xuzh.utils

import com.typesafe.config.ConfigFactory

object ConfigUtils {
  private val config = ConfigFactory.load()

  // 函数
  private val getString:String => (String) = (str) => config.getString(str)
  private val getInt = (str:String) => config.getInt(str)


  // kafka 集群
  val KAFKA_SERVERS = getString("kafka.servers")
  // kafka 主题
  val KAFKA_TOPIC = getString("kafka.streaming.topic")
  // kafka 分组ID
  val kAFKA_CONSUMER_GROUPID = getString("kafka.consumer.group.id")

}
