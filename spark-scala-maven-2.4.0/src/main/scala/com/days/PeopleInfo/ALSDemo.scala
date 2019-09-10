package com.days.PeopleInfo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

import scala.io.Source

object ALSDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //�������ݣ���ת��ΪRDD[Rating]���õ���������
    val conf = new SparkConf().setAppName("UserBaseModel").setMaster("local")
    val sc = new SparkContext(conf)
    val productRatings = loadRatingData("D:\\download\\data\\ratingdata.txt")
    val prodcutRatingsRDD:RDD[Rating] = sc.parallelize(productRatings)
    
    //���һЩ��Ϣ
      val numRatings = prodcutRatingsRDD.count
//    val numUsers = prodcutRatingsRDD.map(x=>x.user).distinct().count
//    val numProducts = prodcutRatingsRDD.map(x=>x.product).distinct().count
//    println("��������" + numRatings +"\t �û�������" + numUsers +"\t ��Ʒ������"+ numProducts)
 
    /*�鿴ALSѵ��ģ�͵�API
        ALS.train(ratings, rank, iterations, lambda)
				����˵����ratings�����־���
				       rank��С�����У����������ĸ������Ƽ��ľ���ֵ�����飺 10~200֮��
				             rankԽ�󣬱�ʾ�����Խ׼ȷ
				             rankԽС����ʾ���ٶ�Խ��
				             
				       iterations:����ʱ�ĵ�����ѭ��������������ֵ��10����
				       lambda��������ϵ����򻯹��̣�ֵԽ�󣬱�ʾ���򻯹���Խ������������ֵԽС��Խ׼ȷ ��ʹ��0.01
    */    
    //val model = ALS.train(prodcutRatingsRDD, 50, 10, 0.01)
    val model = ALS.train(prodcutRatingsRDD, 10, 5, 0.5)
    val rmse = computeRMSE(model,prodcutRatingsRDD,numRatings)
    println("��" + rmse)
    
    
    //ʹ�ø�ģ�ͣ��������Ƽ�
    //����: ���û�1�Ƽ�2����Ʒ                                        �û�ID   ������Ʒ
    val recomm = model.recommendProducts(1, 2)
    recomm.foreach(r=>{ 
      println("�û���" + r.user.toString() +"\t ��Ʒ��"+r.product.toString()+"\t ����:"+r.rating.toString())
    })    
    
    sc.stop()
    
  }
  
    //����RMSE �� ���������
  def computeRMSE(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict((data.map(x => (x.user, x.product))))
    val predictionsAndRating = predictions.map {
      x => ((x.user, x.product), x.rating)
    }.join(data.map(x => ((x.user, x.product), x.rating))).values

    math.sqrt(predictionsAndRating.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)

  }
  
  
  
  
  //��������
  def loadRatingData(path:String):Seq[Rating] = {
    val lines = Source.fromFile(path).getLines()
    
    //���˵�������0������
    val ratings = lines.map(line=>{
        val fields = line.split(",")
        //����Rating�Ķ��� : �û�ID����ƷID����������
        Rating(fields(0).toInt,fields(1).toInt,fields(2).toDouble)
    }).filter(x => x.rating > 0.0)
    
    //ת����  Seq[Rating]
    if(ratings.isEmpty){
      sys.error("Error ....")
    }else{
      //����  Seq[Rating]
      ratings.toSeq
    }
    
  }
}


















