package com.sev7e0.spark.structured_streaming

import java.sql.Timestamp

import org.apache.spark.sql.types.{BooleanType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{Dataset, SparkSession}

object A_1_BasicOperation {

  //DateTime要使用Timestamp  case类必须使用java.sql。在catalyst中作为TimestampType调用的时间戳
  case class DeviceData(device: String, deviceType: String, signal: Double, time: Timestamp)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(A_1_BasicOperation.getClass.getName)
      .master("local")
      .getOrCreate()
    val timeStructType = new StructType().add("device", StringType)
      .add("deviceType", StringType)
      .add("signal", BooleanType)
      .add("time", TimestampType)

    val dataFrame = spark.read.json("src/main/resources/sparkresource/device.json")
    import spark.implicits._
    val ds: Dataset[DeviceData] = dataFrame.as[DeviceData]

    //使用无类型方式查询,类sql
    dataFrame.select("device").where("signal>10").show()
    //使用有类型方式进行查询
    ds.filter(_.signal > 10).map(_.device).show()

    //使用无类型方式进行groupBy,并进行统计
    dataFrame.groupBy("deviceType").count().show()


    import org.apache.spark.sql.expressions.scalalang.typed
    //使用有类型方式进行 计算每种类型的设备的平均信号值
    ds.groupByKey(_.deviceType).agg(typed.avg(_.signal)).show()

    //也可以使用创建临时视图的形式,使用sql语句进行查询
    dataFrame.createOrReplaceTempView("device")
    spark.sql("select * from device").show()

    //可以使用isStreaming来判断是否有流数据
    println(dataFrame.isStreaming)
  }
}

/*
https://blog.csdn.net/lovebyz/article/details/75045514
  val streamingInputDF =
    spark
      .readStream // DataStreamReader
      .format("eventhubs") // DataStreamReader
      .options(eventHubsConf.toMap) // DataStreamReader
      .load() // DataFrame

  // split lines by whitespaces and explode the array as rows of 'word'
  val df = streamingInputDF.select($"body".cast("string"))
    .withColumn("_tmp", split($"body", ";"))
    .select(
      $"_tmp".getItem(0).as("name"),
      $"_tmp".getItem(1).as("ptime")
    ).drop("_tmp")
    .withColumn("posttime", to_timestamp($"ptime", "yyyyMMdd HH:mm:ss"))
    .drop("ptime")
    .withWatermark("posttime", "15 minutes")
    .groupBy(
      window($"posttime", "5 minutes", "5 minutes"),
      $"name"
    )
    .count()
    .writeStream
    .outputMode("update")
    .format("console")
    .start()

  df.awaitTermination()

 */