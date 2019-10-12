package com.sha.sparkmall.mock.util

import java.util.Random

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer
import util.control.Breaks._
/**
  * @author shamin
  * @create 2019-04-09 11:41 
  */
object RandomNum {

  def apply(fromNum: Int, toNum: Int): Int = {
    fromNum + new Random().nextInt(toNum - fromNum + 1)
  }

  def multi(fromNum: Int, toNum: Int, amount: Int, delimiter: String, canRepeat: Boolean) = {
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //用delimiter分割  canRepeat为false则不允许重复
    var str:String = ""
    if (canRepeat) {
      var listBuffer = new ListBuffer[Int]
      while(listBuffer.size < amount){
        var randomNum = fromNum + new Random().nextInt(toNum - fromNum + 1)
        listBuffer += randomNum
      }
      str = listBuffer.mkString(delimiter)
    } else {
      var hashSet = new HashSet[Int]
      while(hashSet.size < amount){
        var randomNum = fromNum + new Random().nextInt(toNum - fromNum + 1)
        hashSet += randomNum
      }
      str = hashSet.mkString(delimiter)
    }
    str
  }

  def main(args: Array[String]): Unit = {
    val str = multi(6,10,5,",",false)
    println(str)
  }
}

