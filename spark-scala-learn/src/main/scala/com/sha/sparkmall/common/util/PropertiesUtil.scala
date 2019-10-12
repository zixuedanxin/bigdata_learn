package com.sha.sparkmall.common.util

import java.io.InputStreamReader
import java.util.Properties

/**
  * @author shamin
  * @create 2019-04-09 11:13 
  */
object PropertiesUtil {

  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtil.load("config.properties")

    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertieName: String): Properties = {
    val prop = new Properties();
    println(prop)
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))
    prop
  }
}
