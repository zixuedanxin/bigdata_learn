package com.bigdata.local.rdd.operation.transformation

import org.apache.spark.{SparkConf, SparkContext}

object FlatMapRun {

  var appName = "worldcount-3"
  var master = "local" //本地模式:local     standalone:spark://master:7077

  def main(args: Array[String]): Unit = {
    val sc = pre()
    val r1 = sc.parallelize(Array(("A",1),("B",2),("C",3)),1)
    println("===============r2")
    val r2 = r1.flatMap(_._1 + "a").foreach(println)
    println("===============r3")
    val r3 = r1.map(_._1 + "a").foreach(println)





    sc.stop()
  }

  def pre(): SparkContext ={
    var startTime = System.currentTimeMillis()
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    conf.set("spark.eventLog.enabled","true")
    conf.set("spark.history.fs.logDirectory","/opt/bigdata/spark-1.6.0-cdh5.15.0/rundata/historyEventLog")
    conf.set("spark.eventLog.dir","/home/spark/log/eventLog")
    conf.setJars(Array("D:\\workspaces\\bigdata\\spark-scala-maven\\target\\spark-scala-maven-1.0-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)
    sc
  }
}