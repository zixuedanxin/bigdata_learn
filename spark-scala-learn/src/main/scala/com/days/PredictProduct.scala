package com.days

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/*
 * 1����δ����˹���ע�����ݣ�
 *     case class LabeledPoint(label: Double, features: Vector)
 *     ������label: �˹���ע������
 *          features: ��������
 *          
 * 2����ν����߼��ع飿
 * 		LogisticRegressionWithLBFGS: ֧�ֶ���࣬���Ը���ľۺϣ��ٶȿ졢Ч���ã�
 *    LogisticRegressionWithSGD  : �ݶ��½�����ֻ֧�ֶ����ࡣ��Spark 2.1�󣬻�����������
 *    
 */
object PredictProduct {
  def main(args: Array[String]): Unit = {
    //���û�������
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)   
    
    val conf = new SparkConf().setMaster("local").setAppName("PredictProduct")
    val sc = new SparkContext(conf)
    
    //�������� ----> ��װLabeledPoint
    val data = sc.textFile("D:\\download\\data\\sales.data")
    val parseData = data.filter(_.length() > 0).map(line =>{
        //�ִʲ��������� @�ִ�
        val parts = line.split("@")
        //����һ��LabeledPoint(label: Double, features: Vector)
        //             �˹���ע������                                       ���е���������
        LabeledPoint(parts(0).trim().toDouble,Vectors.dense(parts(1).split(",").map(_.trim().toDouble)))     
    })
    
    //ִ���߼��ع�                                                                  2��ʾ�Ƕ�����
    //val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(parseData)
    val model = new LogisticRegressionWithSGD().run(parseData)
    
    //�����ϣ���ģ�ͱ��浽HDFS
    //model.save(sc, "hdfs://192.168.157.21:8020/PredictModel")
    
    //ģ���Ѿ�����
    //val model = LogisticRegressionModel.load(sc, "hdfs://192.168.157.21:8020/PredictModel")
    
    
    //ʹ�ø�ģ�ͽ���Ԥ�⣺
    /* �û����������ݣ�
     * 1,23,175,6  ------>  ���ǲ���1.0
     * 0,22,160,5  ------>  ���ǲ���0.0
     */
    val target = Vectors.dense(1,23,175,6)
    //val target = Vectors.dense(0,22,160,5)
    
    //ִ��Ԥ��
    val result = model.predict(target)
    println("���ǲ���" + result)
    
    sc.stop()
  }
}















