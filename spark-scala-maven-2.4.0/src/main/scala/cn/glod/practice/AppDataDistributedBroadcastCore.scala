package cn.glod.practice

import cn.glod.beans.RptAppIspName
import cn.glod.config.ConfigHelper
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * @author zhangjin
  * @create 2018-06-30 11:26
  */
object AppDataDistributedBroadcastCore {

  def main(args: Array[String]): Unit = {

    // 模板代码
    val sparkConf = new SparkConf()
    sparkConf.setAppName("app分布统计-- spark core的实现方式")
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    //获取app数据并且当做广播变量 传递出去
    val appDict: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-dmp/app_dict.txt")
    val tps: RDD[(String, String)] = appDict.map(_.split("\t")).filter(_.size>=5).map(line => (line(4), line(1))).filter(_._1 != null).filter(_._2 != null)
//    val value: RDD[Array[String]] = appDict.map(_.split(" "))
//    val flatten: List[String] = value.collect().toList.flatten
//    flatten.foreach(println)


    //      .map(line => (line(4), line(1))).filter(_._1 != null).filter(_._2 != null)
    val values: Map[String, String] = tps.collect().toMap
    val broadcastAppdict = sc.broadcast(values)
    // 读取parquet文件
    val dataFrame = sqlContext.read.parquet(ConfigHelper.parquetPath)

    import sqlContext.implicits._

    // spark core => reduceByKey =>
    // (K, V) => (K=省+市, V=Seq(原始请求,有效请求,广告请求,参与竞价数,竞价成功数	,展示量,点击量,广告成本,广告消费))
    // K=湖北省+天门市 List(V1) ?? List(V2) ?? List(V3)
    val baseRDD = dataFrame.rdd.map(row => {
      val appid = row.getAs[String]("appid")
      var appname = row.getAs[String]("appname")
      val appDictKeyValue: Map[String, String] = broadcastAppdict.value
      if (StringUtils.isBlank(appname)) {
        if (StringUtils.isNotBlank(appid)) {
          appname = appDictKeyValue.get(appid).getOrElse("未知")
        } else {
          appname = "未知"
        }
      }
      val reqMode = row.getAs[Int]("requestmode")
      val proNode = row.getAs[Int]("processnode")

      // 判断是否是原始请求
      val rawRequest = if (reqMode == 1 && proNode >= 1) 1 else 0
      // 判断是否是有效请求
      val effRequest = if (reqMode == 1 && proNode >= 2) 1 else 0
      // 判断是否是广告请求
      val adRequest = if (reqMode == 1 && proNode == 3) 1 else 0


      val efftive = row.getAs[Int]("iseffective")
      val bill = row.getAs[Int]("isbilling")
      val bid = row.getAs[Int]("isbid")
      val win = row.getAs[Int]("iswin")
      val orderId = row.getAs[Int]("adorderid")

      val winPrice = row.getAs[Double]("winprice")
      val payment = row.getAs[Double]("adpayment")

      // 判断是否是参与竞价的日志
      val rtbRequest = if (efftive == 1 && bill == 1 && bid == 1 && orderId != 0) 1 else 0
      // 判断是否是竞价成功的日志
      val rtbSuccReqAndCostAndExpense: (Int, Double, Double) = if (efftive == 1 && bill == 1 && win == 1) (1, winPrice / 1000, payment / 1000) else (0, 0d, 0d)


      // 判断是否是广告展示的日志
      val adShow = if (reqMode == 2 && efftive == 1) 1 else 0
      // 判断是否是广告点击的日志
      val adClick = if (reqMode == 3 && efftive == 1) 1 else 0


      // Seq(原始请求,有效请求,广告请求,参与竞价数,竞价成功数	,展示量,点击量,广告成本,广告消费)
      (
        appname,
        Seq(
          rawRequest, effRequest, adRequest, rtbRequest,
          rtbSuccReqAndCostAndExpense._1, adShow, adClick,
          rtbSuccReqAndCostAndExpense._2, rtbSuccReqAndCostAndExpense._3
        )
      )
    }).cache()


    baseRDD.map(tp => (tp._1, tp._2)).reduceByKey {
      case (seq1, seq2) => seq1.zip(seq2).map(tp => tp._1 + tp._2)
    }.map(tp => RptAppIspName(
      tp._1, tp._2(0).toInt,
      tp._2(1).toInt, tp._2(2).toInt, tp._2(3).toInt,
      tp._2(4).toInt, tp._2(5).toInt, tp._2(6).toInt, tp._2(7), tp._2(8))).toDF().show()


  }

}
