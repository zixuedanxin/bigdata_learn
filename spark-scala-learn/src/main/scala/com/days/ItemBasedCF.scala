package com.days

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, MatrixEntry, RowMatrix}
import org.apache.spark.rdd.RDD

/*
 * ������Ʒ�����ƶȣ��������Ƽ�
 */
object ItemBasedCF {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //��������
    val conf = new SparkConf().setAppName("UserBaseModel").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("D:\\download\\data\\ratingdata.txt")

    /*MatrixEntry����һ���ֲ�ʽ�����е�ÿһ��(Entry)
     * �����ÿһ���һ��(i: Long, j: Long, value: Double) ָʾ����ֵ��Ԫ��tuple��
     * ����i�������꣬j�������꣬value��ֵ��*/
    val parseData: RDD[MatrixEntry] =
      data.map(_.split(",") match { case Array(user, item, rate) => MatrixEntry(user.toLong, item.toLong, rate.toDouble) })

    //CoordinateMatrix��Spark MLLib��ר�ű���user_item_rating��������������
    val ratings = new CoordinateMatrix(parseData)

    /* ����CoordinateMatrixû��columnSimilarities����������������Ҫ����ת����RowMatrix���󣬵�������columnSimilarities������������
     * RowMatrix�ķ���columnSimilarities�Ǽ��㣬�����е����ƶȣ�������user_item_rating��������û���CF��ͬ���ǣ����ﲻ��Ҫ���о����ת�ã�ֱ�Ӿ�����Ʒ������*/
    val matrix: RowMatrix = ratings.toRowMatrix()

    //����Ϊĳһ���û��Ƽ���Ʒ���������߼��ǣ����ȵõ�ĳ���û����۹������������Ʒ��Ȼ�����������Ʒ�����Ʒ�����ƶȣ������򣻴Ӹߵ��ͣ��Ѳ����û����۹�
    //��Ʒ���������Ʒ�Ƽ����û���
    //���磺Ϊ�û�2�Ƽ���Ʒ

    //��һ�����õ��û�2���۹������������Ʒ  take(5)��ʾȡ�����е�5���û�  2:��ʾ�ڶ����û�
    //���ͣ�SparseVector��ϡ�����
    val user2pred = matrix.rows.take(5)(2)
    val prefs: SparseVector = user2pred.asInstanceOf[SparseVector]
    val uitems = prefs.indices //�õ����û�2���۹������������Ʒ��ID   
    val ipi = (uitems zip prefs.values) //�õ����û�2���۹������������Ʒ��ID�����֣�����(��ƷID,����)   
//    for (s <- ipi) println(s)
//    println("*******************")


    //������Ʒ�������ԣ������
    val similarities = matrix.columnSimilarities()
    val indexdsimilar = similarities.toIndexedRowMatrix().rows.map {
      case IndexedRow(idx, vector) => (idx.toInt, vector)
    }
//    indexdsimilar.foreach(println)
//    println("*******************")
    
    //ij��ʾ�������û��������Ʒ���û�2����ĸ���Ʒ�����ƶ�
    val ij = sc.parallelize(ipi).join(indexdsimilar).flatMap {
      case (i, (pi, vector: SparseVector)) => (vector.indices zip vector.values)
    }

    //ij1��ʾ�������û���������������û�2�������Ʒ���б��е���Ʒ������
    val ij1 = ij.filter { case (item, pref) => !uitems.contains(item) }
    //ij1.foreach(println)
    //println("*******************")

    //����Щ��Ʒ��������ͣ����������У����Ƽ�ǰ������Ʒ
    val ij2 = ij1.reduceByKey(_ + _).sortBy(_._2, false).take(2)
    println("********* �Ƽ��Ľ���� ***********")
    ij2.foreach(println)
  }
}

