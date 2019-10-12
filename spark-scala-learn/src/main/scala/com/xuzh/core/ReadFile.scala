package com.xuzh.core

import com.xuzh.utils.MySparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types._

/**
  * @author zhangjin
  */
object ReadFile {

  def main(args: Array[String]): Unit = {

    //第一步 读取数据
    val session: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()

    //导入SparkSession上面的隐士转换
    import session.implicits._
    //    val sc: SparkContext = MySparkUtils.getLocalSparkContext(this.getClass().getSimpleName)
    //    val sqlContext: SQLContext =  new SQLContext(sc)


    val file: RDD[String] = session.sparkContext.textFile("/Users/zhangjin/myCode/learn/spark-dmp/2016-10-01_06_p1_invalid.1475274123982.log")

    file.collect().foreach(println)


    val rowRdd: RDD[Row] = file.map(_.split(",")).filter(_.size > 84).map(
      t => Row(
        t(0),
        if (t(1) != null && t(1) != "") t(1).toInt else 0,
        if (t(2) != null && t(2) != "") t(2).toInt else 0,
        t(3).toInt,
        t(4).toInt,
        t(5),
        t(6),
        t(7).toInt,
        t(8).toInt,
        t(9).toDouble,
        t(10).toDouble,
        t(11),
        t(12),
        t(13),
        t(14),
        t(15),
        t(16),
        t(17).toInt,
        t(18),
        t(19),
        if (t(20) != null && t(20) != "") t(20).toInt else 0,
        if (t(21) != null && t(21) != "") t(21).toInt else 0,
        t(22),
        t(23),
        t(24),
        t(25),
        t(26).toInt,
        t(27),
        t(28).toInt,
        t(29),
        t(30).toInt,
        t(31).toInt,
        t(32).toInt,
        t(33),
        t(34).toInt,
        t(35).toInt,
        t(36).toInt,
        t(37),
        t(38).toInt,
        t(39).toInt,
        t(40).toDouble,
        t(41).toDouble,
        t(42).toInt, t(43),
        if (t(44) != null && t(44) != "") t(44).toDouble else 0.0d,
        t(45).toDouble,
        t(46),
        t(47),
        t(48),
        t(49),
        t(50),
        t(51),
        t(52),
        t(53),
        t(54),
        t(55),
        t(56),
        t(57).toInt,
        t(58).toDouble,
        t(59).toInt,
        t(60).toInt,
        t(61),
        t(62),
        t(63),
        t(64),
        t(65),
        t(66),
        t(67),
        t(68),
        t(69),
        t(70),
        t(71),
        t(72),
        t(73).toInt,
        t(74).toDouble,
        t(75).toDouble,
        if (t(76) != null && t(76) != "") t(76).toDouble else 0.0d,
        if (t(77) != null && t(77) != "") t(77).toDouble else 0.0d,
        if (t(78) != null && "".equals(t(78))) t(78).toDouble else 0.0d,
        t(79),
        t(80),
        t(81),
        t(82),
        t(83),
        t(84).toInt

      )
    )


    //    val rowRdd: RDD[Row] = file.map(_.split(",")).filter(_.size > 85).map(
    //      t => Row(
    //        t(0), t(1), t(2),t(3),t(4),t(5), t(6), t(7), t(8),t(9),
    //        t(10), t(11), t(12),t(13),t(14),t(15), t(16), t(17), t(18),t(19),
    //        t(20), t(21),t(22),t(23),t(24), t(25), t(26), t(27),t(28), t(29),
    //        t(30) ,t(31), t(32),t(33),t(34),t(35), t(36), t(37), t(38),t(39),
    //        t(40), t(41), t(42),t(43),t(44),t(45), t(46), t(47), t(48),t(49),
    //        t(50), t(51), t(52),t(53),t(54),t(55), t(56), t(57), t(58),t(59),
    //        t(60), t(61), t(62),t(63),t(64),t(65), t(66), t(67), t(68),t(69),
    //        t(70), t(71), t(72),t(73),t(74),t(75), t(76), t(77), t(78),t(79),
    //        t(80), t(81), t(82),t(83),t(84),t(85), t(86), t(87)
    //
    //      )
    //    )


    //自定义schema信息
    val schema: StructType = StructType(
      List(
        //字段的名称 类型 是否为空
        StructField("sessionid", StringType, true),
        StructField("advertisersid", IntegerType, true),
        StructField("adorderid", IntegerType, true),
        StructField("adcreativeid", IntegerType, true),
        StructField("adplatformproviderid", IntegerType, true),
        StructField("sdkversion", StringType, true),
        StructField("adplatformkey", StringType, true),
        StructField("putinmodeltype", IntegerType, true),
        StructField("requestmode", IntegerType, true),
        StructField("adprice", DoubleType, true),

        StructField("adppprice", DoubleType, true),
        StructField("requestdate", StringType, true),
        StructField("ip", StringType, true),
        StructField("appid", StringType, true),
        StructField("appname", StringType, true),
        StructField("uuid", StringType, true),
        StructField("device", StringType, true),
        StructField("client", IntegerType, true),
        StructField("osversion", StringType, true),
        StructField("density", StringType, true),

        StructField("pw", IntegerType, true),
        StructField("ph", IntegerType, true),
        StructField("long", StringType, true),
        StructField("lat", StringType, true),
        StructField("provincename", StringType, true),
        StructField("cityname", StringType, true),
        StructField("ispid", IntegerType, true),
        StructField("ispname", StringType, true),
        StructField("networkmannerid", IntegerType, true),
        StructField("networkmannername", StringType, true),

        StructField("iseffective", IntegerType, true),
        StructField("isbilling", IntegerType, true),
        StructField("adspacetype", IntegerType, true),
        StructField("adspacetypename", StringType, true),
        StructField("devicetype", IntegerType, true),
        StructField("processnode", IntegerType, true),
        StructField("apptype", IntegerType, true),
        StructField("district", StringType, true),
        StructField("paymode", IntegerType, true),
        StructField("isbid", IntegerType, true),

        StructField("bidprice", DoubleType, true),
        StructField("winprice", DoubleType, true),
        StructField("iswin", IntegerType, true),
        StructField("cur", StringType, true),
        StructField("lrate", DoubleType, true),
        StructField("cnywinprice", DoubleType, true),
        StructField("imei", StringType, true),
        StructField("mac", StringType, true),
        StructField("idfa", StringType, true),
        StructField("openudid", StringType, true),

        StructField("androidid", StringType, true),
        StructField("rtbprovince", StringType, true),
        StructField("rtbcity", StringType, true),
        StructField("rtbdistrict", StringType, true),
        StructField("rtbstreet", StringType, true),
        StructField("storeurl", StringType, true),
        StructField("realip", StringType, true),
        StructField("isqualityapp", IntegerType, true),
        StructField("bidfloor", DoubleType, true),
        StructField("aw", IntegerType, true),

        StructField("ah", IntegerType, true),
        StructField("imeimd5", StringType, true),
        StructField("macmd5", StringType, true),
        StructField("idfamd5", StringType, true),
        StructField("openudidmd5", StringType, true),
        StructField("androididmd5", StringType, true),
        StructField("imeisha1", StringType, true),
        StructField("macsha1", StringType, true),
        StructField("idfasha1", StringType, true),
        StructField("openudidsha1", StringType, true),

        StructField("androididsha1", StringType, true),
        StructField("uuidunknow", StringType, true),
        //        StructField("decuuidunknow", StringType, true),
        StructField("userid", StringType, true),
        //        StructField("reqdate", StringType, true),
        //        StructField("reqhour", StringType, true),
        StructField("iptype", IntegerType, true),
        StructField("initbidprice", DoubleType, true),
        StructField("adpayment", DoubleType, true),
        StructField("agentrate", DoubleType, true),

        StructField("rate2", DoubleType, true),
        StructField("adxrate", DoubleType, true),
        StructField("title", StringType, true),
        StructField("keywords", StringType, true),
        StructField("tagid", StringType, true),
        StructField("callbackdate", StringType, true),
        StructField("channelid", StringType, true),
        StructField("mediatype", IntegerType, true)
      ))


    val dataFrame: DataFrame = session.sqlContext.createDataFrame(rowRdd, schema)
    dataFrame.registerTempTable("logs")

    dataFrame.write.format("parquet").save("/Users/zhangjin/myCode/learn/spark-dmp/dmp.parquet")
    //    dataFrame.to

    session.close()
  }

}
