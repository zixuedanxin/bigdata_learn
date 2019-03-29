package com.bigdata.spark.local

import org.apache.spark.{SparkConf, SparkContext}

object LocalRunTextFileCount{

  var appName = "localTest"
  var master = "local[2]" //本地模式:local

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    conf.set("spark.storage.blockManagerSlaveTimeoutMs","10000s")
    conf.set("spark.storage.blockManagerSlaveTimeoutMs","10000s")
    conf.set("spark.executor.heartbeatInterval","10000s")
    conf.set("spark.network.timeout","10000ms")
    conf.set("spark.executor.heartbeat.maxFailures","10000000")
    val sc = new SparkContext(conf)
    val distFile:org.apache.spark.rdd.RDD[String] = sc.textFile("src/main/resource/data/text/people.txt")
    println("===================")
    println(distFile)
    distFile.foreach(r => println(Thread.currentThread().getId + Thread.currentThread().getName,r))
    val threadName = Thread.currentThread().getId + Thread.currentThread().getName
   println(s" $threadName 结果 count:${distFile.count()}")

  }
}
