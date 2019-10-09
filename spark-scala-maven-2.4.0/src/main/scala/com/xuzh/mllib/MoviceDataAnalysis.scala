package com.xuzh.mllib

import breeze.linalg.{DenseVector, sum}
import breeze.numerics.pow
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object MoviceDataAnalysis {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local[4]").setAppName("MoviceDataAnalysis")
    val sc = new SparkContext(conf)

    /*加载电影信息*/
    val file_item = sc.textFile("D:\\原始数据\\ml-100k\\u.item")
    println(file_item.first())
    /*加载电影类别信息*/
    val file_genre=sc.textFile("D:\\原始数据\\ml-100k\\u.genre")
    println(file_genre.first())
    /*加载评论人的信息*/
    val file_user=sc.textFile("D:\\原始数据\\ml-100k\\u.user")
    println(file_user.first())
    /*加载评论人的评论信息*/
    val file_data=sc.textFile("D:\\原始数据\\ml-100k\\u.data")
    println(file_data.first())

    /*训练模型*/
    val data_vector = file_data.map(_.split("\t")).map{
      x =>
        Rating(x(0).toInt,x(1).toInt,x(2).toDouble)
    }.cache()

    val alsModel = ALS.train(data_vector,50,10,0.1)

    /*获取用户相似特征*/
    val userFactors = alsModel.userFeatures

    /*获取商品相似度特征*/
    val movieFactors = alsModel.productFeatures

    /*商品相似特征向量化*/
    val movieVectors = movieFactors.map(x => Vectors.dense(x._2))

    /*用户相似特征向量化*/
    val userVectors = userFactors.map(x => Vectors.dense(x._2))

    /*对向量特征进行归一化判断*/
    val movieMatrix=new RowMatrix(movieVectors)
    val movieMatrix_Summary=movieMatrix.computeColumnSummaryStatistics()
    println(movieMatrix_Summary.mean)//每列的平均值
    println(movieMatrix_Summary.variance)//每列的方差
    val userMatrix=new RowMatrix(userVectors)
    val userMatrix_Summary=userMatrix.computeColumnSummaryStatistics()
    println(userMatrix_Summary.mean)//每列的平均值
    println(userMatrix_Summary.variance)//每列的方差

    /*对用户K-means因子聚类*/
    val userClusterModel=KMeans.train(userVectors,5,100)
    /*使用聚类模型进行预测*/
    val user_predict=userClusterModel.predict(userVectors)
    def computeDistance(v1:DenseVector[Double],v2:DenseVector[Double])=sum(pow(v1-v2,2))
    user_predict.map(x =>(x,1)).reduceByKey(_+_).collect().foreach(println(_))

    println("========================")

    val userInfo=file_user.map(_.split("\\|")).map{
      x => (x(0).toInt,(x(1),x(2),x(3),x(4)))
    }
    /*联合用户信息和特征值*/
    val infoAndFactors=userInfo.join(userFactors)
    val userAssigned=infoAndFactors.map{
      case(userId,((age,sex,title,zip),factors)) =>
        val pred=userClusterModel.predict(Vectors.dense(factors))
        val center=userClusterModel.clusterCenters(pred)
        val dist=computeDistance(DenseVector(factors),DenseVector(center.toArray))
        (userId,age,sex,title,zip,dist,pred)
    }
    val userCluster=userAssigned.groupBy(_._7).collectAsMap()
    /*输出每个类中的20个用户分类情况*/
    for((k,v) <- userCluster.toSeq.sortBy(_._1)){
      println(s"userCluster$k")
      val info=v.toSeq.sortBy(_._6)
      println(info.take(20).map{
        case(userId,age,sex,title,zip,pred,dist) =>
          (userId,age,sex,title,zip)
      }.mkString("\n"))
      println("========================")
    }

    /*对电影K-means因子聚类*/
    val movieClusterModel=KMeans.train(movieVectors,5,100)
    /*KMeans: KMeans converged in 39 iterations.*/
    val movie_predict=movieClusterModel.predict(movieVectors)
    movie_predict.map(x =>(x,1)).reduceByKey(_+_).collect.foreach(println(_))
    println("========================")
    /*查看及分析商品相似度聚类数据*/
    /*提取电影的题材标签*/
    val genresMap=file_genre.filter(!_.isEmpty).map(_.split("\\|"))
      .map(x => (x(1),x(0))).collectAsMap()
    /*为电影数据和题材映射关系创建新的RDD，其中包含电影ID、标题和题材*/
    val titlesAndGenres=file_item.map(_.split("\\|")).map{
      array =>
        val geners=array.slice(5,array.size).zipWithIndex.filter(_._1=="1").map(
          x => genresMap(x._2.toString)
        )
        (array(0).toInt,(array(1),geners))
    }
    val titlesWithFactors=titlesAndGenres.join(movieFactors)
    val movieAssigned=titlesWithFactors.map{
      case(id,((movie,genres),factors)) =>
        val pred=movieClusterModel.predict(Vectors.dense(factors))
        val center=movieClusterModel.clusterCenters(pred)
        val dist=computeDistance(DenseVector(factors),DenseVector(center.toArray))
        (id,movie,genres.mkString(" "),pred,dist)
    }
    val clusterAssigned=movieAssigned.groupBy(_._4).collectAsMap()
    for((k,v)<- clusterAssigned.toSeq.sortBy(_._1)){
      println(s"Cluster$k")
      val dist=v.toSeq.sortBy(_._5)
      println(dist.take(20).map{
        case (id,movie,genres,pred,dist) =>
          (id,movie,genres)
      }.mkString("\n"))
      println("============")
    }

    /*内部评价指标，计算性能*/
    val movieCost=movieClusterModel.computeCost(movieVectors)
    println(movieCost)
    val userCost=movieClusterModel.computeCost(userVectors)
    println(userCost)



  }

}
