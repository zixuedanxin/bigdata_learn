package cn.huage.practice

import cn.huage.config.ConfigHelper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhangjin
  * @create 2018-06-29 21:54
  */
object ProvinceDataDistributedCore {


  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf()
    sparkConf.setAppName("各省市的地域分布统计")
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sparkContext = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sparkContext)

    // 读取数据 parquet
    val dataFrame = sqlContext.read.parquet(ConfigHelper.parquetPath)

//    dataFrame.rdd.collect().toArray.foreach(println)

    val datas: RDD[((String, String), (Int, Int, Int, Int, Double, Double))] = dataFrame.rdd.map(row => {
      val pName = row.getAs[String]("provincename")
      val cName = row.getAs[String]("cityname")
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val adpayment: Double = row.getAs[Double]("adpayment")
      val winprice: Double = row.getAs[Double]("winprice")

      println(iseffective + " " + isbid + " " + isbilling + "" + adorderid + " " + requestmode)

      var joinBidding = 0
      var sucessBidding = 0
      var display = 0
      var onclick = 0
      var winPrices = 0d
      var adpayments = 0d
      if (iseffective == 1 && isbid == 1 && isbilling == 1 && adorderid != 0) {
        joinBidding = 1
      }
      if (iseffective == 1 && isbid == 1 && iswin == 1) {
        sucessBidding = 1
        winPrices = winprice
        adpayments = adpayment
      }
      if ( requestmode == 2 && iseffective == 1) {
        display = 1
      }
      if (requestmode == 3 && iseffective == 1) {
        onclick = 1
      }
      ((pName, cName), (joinBidding, sucessBidding, display, onclick, winPrices, adpayments))
    })

    datas.collect().foreach(println)

    val dataRes: RDD[((String, String), (Int, Int, Int, Int, Double, Double))] = datas.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + a._5, a._6 + b._6))
    dataRes.saveAsTextFile("/Users/zhangjin/myCode/learn/spark-dmp/output/report1")


    sparkContext.stop()

  }
}
