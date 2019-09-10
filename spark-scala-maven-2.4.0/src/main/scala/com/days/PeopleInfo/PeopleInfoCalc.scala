package com.days.PeopleInfo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PeopleInfoCalc {
  def main(args: Array[String]): Unit = {
    //����SparkContext
    //val conf = new SparkConf().setAppName("PeopleInfoCalc").setMaster("local")
    val conf = new SparkConf().setAppName("PeopleInfoCalc")
    val sc = new SparkContext(conf)
    
    //��ȡ����
    //val dataFile = sc.textFile("d:\\temp\\sample_people_info.txt")
    val dataFile = sc.textFile(args(0))
    
    //��֣��С�Ů                 ����     2 M 105             �Ա�                       ���
    val maleData = dataFile.filter(line=>line.contains("M")).map(line=>(line.split(" ")(1) + " " + line.split(" ")(2)))
    val feMaleData = dataFile.filter(line=>line.contains("F")).map(line=>(line.split(" ")(1) + " " + line.split(" ")(2)))
    
    //�õ����Ե����
    val maleHeighData = maleData.map(line=>line.split(" ")(1).toInt)
    val femaleHeighData = feMaleData.map(line=>line.split(" ")(1).toInt)
    
    //��ߺ����ֵ
    val lowestMale = maleHeighData.sortBy(x=>x, true).first()
    val lowestFeMale = femaleHeighData.sortBy(x=>x, true).first() 
    
    val highesttMale = maleHeighData.sortBy(x=>x, false).first()
    val highestFeMale = femaleHeighData.sortBy(x=>x, false).first() 
    
    //���
    println("lowestMale:" + lowestMale)
    println("lowestFeMale:" + lowestFeMale)
    println("highesttMale:" + highesttMale)
    println("highestFeMale:" + highestFeMale)
    
    sc.stop()
  }
}

















