package com.days

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.rdd.RDD


/*
 * 1�������û������ƶȾ���
 * 2������һЩ����
 */
object UserBasedCF {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)   
    
    //����һ��SparkContext
    val conf = new SparkConf().setAppName("BlackUserList").setMaster("local")
    val sc = new SparkContext(conf)
    
    //��������
    val data = sc.textFile("D:\\download\\data\\ratingdata.txt")
    //MatrixEntry���������е�һ��
    //ʹ��ģʽƥ��
    val parseData:RDD[MatrixEntry] = data.map(_.split(",") 
        match {case Array(user,item,rate) => MatrixEntry(user.toLong,item.toLong,rate.toDouble)} )
        
    //�������־��� new CoordinateMatrix(entries: RDD[MatrixEntry]) 
    val ratings = new CoordinateMatrix(parseData)
    
    //�����û������ƶȾ���:��Ҫ���о����ת��
    val matrix:RowMatrix = ratings.transpose().toRowMatrix()
    
    //���м��㣬�õ����û������ƶȾ���
    val similarities = matrix.columnSimilarities()
    println("����û����ƶȾ���")
    similarities.entries.collect().map(x=>{
      println(x.i +" --->" + x.j+ "  ----> " + x.value)
    })
    
    println("-----------------------------------------")
    //�õ�ĳ���û���������Ʒ�����֣��û�1Ϊ��
    val ratingOfUser1 = ratings.entries.filter(_.i == 1).map(x=>(x.j,x.value)).sortBy(_._1).collect().map(_._2).toList.toArray
    println("�û�1��������Ʒ������")
    for(s<-ratingOfUser1) println(s)
    
    println("-----------------------------------------")
    
    //�õ��û�1����������û���������
    val similarityOfUser1 = similarities.entries.filter(_.i == 1).sortBy(_.value,false).map(_.value).collect
    println("�û�1����������û���������")
    for(s<- similarityOfUser1) println(s)
      
    
    sc.stop()
  }
}



















