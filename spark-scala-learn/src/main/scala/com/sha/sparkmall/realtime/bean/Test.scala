package com.sha.saprkmall.realtime.bean

/**
  * @author shamin
  * @create 2019-04-13 10:36 
  */
object Test {
  def main(args: Array[String]): Unit = {
    val stringToTuples: Map[String, Iterable[(String, (String, Int))]] = Iterable(("a",("ad1",100)),("b",("ad2",100)),("a",("ad3",100))).groupBy{case (a,(b,c)) => a}
    for (elem <- stringToTuples) {
      println(elem)
    }
  }
}
