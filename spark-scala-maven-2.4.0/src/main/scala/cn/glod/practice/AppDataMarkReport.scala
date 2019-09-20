package cn.glod.practice


import cn.glod.config.ConfigHelper
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer

/**
  * @author zhangjin
  * @create 2018-07-01 15:58
  */
object AppDataMarkReport {

  def main(args: Array[String]): Unit = {

    //初始化配置文件
    val sparkConf = new SparkConf()
    sparkConf.setAppName("app数据标签化-- spark core的实现方式")
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    //获取app数据并且当做广播变量 传递出去
    val appDict: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-dmp/app_dict.txt")
    val tps: RDD[(String, String)] = appDict.map(_.split("\t")).filter(_.size >= 5).map(line => (line(4), line(1))).filter(_._1 != null).filter(_._2 != null)
    val values: Map[String, String] = tps.collect().toMap
    val broadcastAppdict = sc.broadcast(values)
    // 读取parquet文件
    val dataFrame = sqlContext.read.parquet(ConfigHelper.parquetPath)
    import sqlContext.implicits._
    // spark core => reduceByKey =>
    // (K, V) => (K=省+市, V=Seq(原始请求,有效请求,广告请求,参与竞价数,竞价成功数	,展示量,点击量,广告成本,广告消费))
    // K=湖北省+天门市 List(V1) ?? List(V2) ?? List(V3)
    val baseRDD: RDD[(String, ArrayBuffer[(String, Int)])] = dataFrame.rdd.map(row => {
      //整体封装成一个 （key，List）的形式   key是userid 其他的类型如果有值 就添加进List
      val array = new ArrayBuffer[(String, Int)]()
      //1 获取 设备id逻辑  imei idfa mac androidid openudid 第一个不为空的就是他并且Md5 暂时不考虑md5 最后

      val imei: String = row.getAs[String]("imei")
      val idfa: String = row.getAs[String]("idfa")
      val mac: String = row.getAs[String]("mac")
      val androidid: String = row.getAs[String]("androidid")
      val openudid: String = row.getAs[String]("openudid")
      var userid = "未知"
      if (imei != null && imei != "") {
        userid = imei
      } else if (idfa != null && idfa != "") {
        userid = idfa
      } else if (mac != null && mac != "") {
        userid = mac
      } else if (androidid != null && androidid != "") {
        userid = androidid
      } else if (openudid != null && openudid != "") {
        userid = openudid
      }
      //2 appName 名称
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
      //2 广告位类型
      //广告位类型id (1: banner      2:插屏 3:全屏)
      //广告位类型名称(    banner  、插屏、全屏)
      val adspacetypename = row.getAs[String]("adspacetypename")
      if (adspacetypename != null && adspacetypename != "") {
        array.+=((adspacetypename, 1))
      }
      //3 渠道 adplatformproviderid

      val adplatformproviderid = row.getAs[Int]("adplatformproviderid")
      if (adplatformproviderid != null ) {
        array.+=(("CN"+adplatformproviderid, 1))
      }


      //4 设备操作系统 字段 client 操作系统 networkmannername 联网方式 ispname 运营商
      //1 Android   D00010001 2 IOS       D00010002 WinPhone D00010003 _ 其他 D00010004
      //设备联网方式 WIFI D00020001 4G D00020002 3G D00020003 2G D00020004 _ D00020005
      //设备运营商方式 移动 D00030001 联通 D00030002 电信 D00030003 _ D00030004

      val client = row.getAs[Int]("client")
      val networkname = row.getAs[String]("networkmannername")
      val ispname = row.getAs[String]("ispname")


      val os = client match {
        case 1 => "D00010001"
        case 2 => "D00010002"
        case 3 => "D00010003"
        case _ => "D00010004"
      }
      val network = networkname match {
        case networkname if (networkname != null && networkname.equals("WIFI")) => "D00020001"
        case networkname if (networkname != null && networkname.equals("4G")) => "D00020002"
        case networkname if (networkname != null && networkname.equals("3G")) => "D00020003"
        case networkname if (networkname != null && networkname.equals("2G")) => "D00020004"
        case _ => "D00020005"
      }
      val isp = ispname match {
        case ispname if (ispname != null && ispname.equals("移动")) => "D00030001"
        case ispname if (ispname != null && ispname.equals("联通")) => "D00030002"
        case ispname if (ispname != null && ispname.equals("电信")) => "D00030003"
        case _ => "D00030004"
      }

      if (os != null && os != "") {
        array.+=((os, 1))
      }
      if (network != null && network != "") {
        array.+=((network, 1))
      }
      if (isp != null && isp != "") {
        array.+=((isp, 1))
      }
      //5 关键字 keyWords(标签格式:K   ->1)    为关键字，关键字个数不能少于 3 个字符，
      // 且不能超过 8 个字符;关键字中 如包含‘‘|’’，则分割成数组，转化成多个关键字标签
      val keywords = row.getAs[String]("keywords")
      val words: Array[String] = keywords.split("|")
      words.foreach(k => {
        if (k != null && k.length >= 3 && k.length <= 8) {
          array.+=((k, 1))
        }
      })



      //6 地域标签(省标签格式:ZP   ->1, 地市标签格式: ZC   ->1)    为省或市名称

      val pName = row.getAs[String]("provincename")
      val cName = row.getAs[String]("cityname")
      if (pName != null && pName != "") {
        array.+=((pName, 1))
      }
      if (cName != null && cName != "") {
        array.+=((cName, 1))
      }
      // Seq(原始请求,有效请求,广告请求,参与竞价数,竞价成功数	,展示量,点击量,广告成本,广告消费)

      (userid, array)
    }).cache()

    val result: RDD[(String, List[(String, Int)])] = baseRDD.groupByKey().map(kv => {
      val groupedData: Map[String, Iterable[(String, Int)]] = kv._2.flatten.groupBy(_._1)
      val mapedData: Map[String, Int] = groupedData.mapValues(v => {
        val values: Iterable[Int] = v.map(tp => tp._2)
        values.toList.sum
      })
      (kv._1, mapedData.toList)
    })
    result.saveAsTextFile("/Users/zhangjin/myCode/learn/spark-dmp/output/report2")


    //
    //    baseRDD.map(tp => (tp._1, tp._2)).reduceByKey {
    //      case (seq1, seq2) => seq1.zip(seq2).map(tp => tp._1 + tp._2)
    //    }.map(tp => RptAppIspName(
    //      tp._1, tp._2(0).toInt,
    //      tp._2(1).toInt, tp._2(2).toInt, tp._2(3).toInt,
    //      tp._2(4).toInt, tp._2(5).toInt, tp._2(6).toInt, tp._2(7), tp._2(8))).toDF().show()
    //


    //关闭资源连接
    sc.stop()

  }

}
