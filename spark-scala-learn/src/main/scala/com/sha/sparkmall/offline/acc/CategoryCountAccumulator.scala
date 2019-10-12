package com.sha.sparkmall.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * @author shamin
  * @create 2019-04-09 19:53 
  */
//累加器中泛型
//第一个泛型是add()方法的泛型
//第二个泛型为value()返回值方法的泛型
//这里是根据cid_action 进行累加
class CategoryCountAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {
  var categoryCountMap = new mutable.HashMap[String, Long]()

  //判断是否为空
  override def isZero: Boolean = categoryCountMap.isEmpty

  //driver端
  //复制一个累加器，在driver端复制一个累加器发送到executor端
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator = new CategoryCountAccumulator
    accumulator.categoryCountMap ++= this.categoryCountMap
    accumulator
  }

  //executor端
  //重置累加器
  override def reset(): Unit = {
    categoryCountMap = new mutable.HashMap[String, Long]()
  }

  //executor端
  //累加器
  override def add(v: String): Unit = {
    //如果传入的v不存在，则默认为0 +1 ,存在则取出值 再 +1
    categoryCountMap(v) = categoryCountMap.getOrElse(v, 0L) + 1L
  }

  //driver端
  //合并结果
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    //map合并  key相同的value相加
    val otherMap: mutable.HashMap[String, Long] = other.value
    //将map中的每个
    categoryCountMap = categoryCountMap.foldLeft(otherMap) { case (otherMap, (k, v)) =>
      otherMap(k) = otherMap.getOrElse(k, 0L) + v
      otherMap
    }
  }

  //得到返回值结果
  override def value: mutable.HashMap[String, Long] = {
    this.categoryCountMap
  }
}
