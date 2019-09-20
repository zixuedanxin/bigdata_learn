package com.sha.sparkmall.offline.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap


/**
  * @author shamin
  * @create 2019-04-10 20:56 
  */
class CityRemarkUDAF extends UserDefinedAggregateFunction{

  //功能：聚合函数cityRemark(cityNam) => 北京21.2%，天津13.2%，其他65.6%  group by 地区
  //类比sum(字段)
  //这里统计的是城市最后占的百分比
  //我们要从需要的结果然后进行反推，北京21.2%，天津13.2%，其他65.6%
  //那么统计的就是城市的点击次数
  //cityRemark(cityName)  => 每进来一个cityName就将对应的城市的点击次数+1，并将对应的总的点击次数+1，方便求占的百分比
  //那么就能得出输入类型为cityName的类型(String)和输出类型也为(String)将结果拼接成一个字符串

  //定义输入参数的类型 :StructType(Array[StructField]) 创建一个StructType
  //StructField("inputType",StringType)为创建一个StructField
  override def inputSchema: StructType = StructType(Array(StructField("inputType",StringType)))

  //中间结果的缓存类型
  override def bufferSchema: StructType = StructType(Array(StructField("total",LongType),StructField("cityCount",MapType(StringType,LongType))))

  //返回结果的类型也就是输出类型
  override def dataType: DataType = StringType

  //如果输入的数据相同，是否返回的结果也相同。即是否为确定性函数
  //没有时间戳等，一般为true
  override def deterministic: Boolean = true

  //初始化中间结果
  //在UDAF函数中只能用不可变的map,具体原因不明
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = new HashMap[String,Long]()
  }

  //实现每进来一个cityName就将对应的城市的点击次数+1，并将对应的总的点击次数+1
  //对每个输入行做处理
  //分区内累加值
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val cityName: String = input.getString(0)
    val cityMap: collection.Map[String, Long] = buffer.getMap[String,Long](1)
    val total: Long = buffer.getLong(0)

    //更新总的点击次数
    buffer(0) = total + 1L
    //更新city的点击次数
    buffer(1) = cityMap + (cityName -> (cityMap.getOrElse(cityName,0L) + 1L))

  }

  //合并两个聚合缓冲区并将更新的缓冲区值存储回“buffer1”。
  //当我们将两个部分聚合的数据合并在一起时调用此方法
  //分区间合并值
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val cityMap1: collection.Map[String, Long] = buffer1.getMap[String,Long](1)
    val total1: Long = buffer1.getLong(0)

    val cityMap2: collection.Map[String, Long] = buffer2.getMap[String,Long](1)
    val total2: Long = buffer2.getLong(0)

    //将分区间总的点击次数聚合
    buffer1(0) = total1 + total2
    //分区间两个map聚合，用foldLeft
    buffer1(1) = cityMap1.foldLeft(cityMap1)({case (cityMap1,(key,value)) =>
        cityMap1 + (key -> (cityMap1.getOrElse(key,0L) + value))
    })
  }

  //做最终的计算
  override def evaluate(buffer: Row): Any = {
    val cityMap: collection.Map[String, Long] = buffer.getMap[String,Long](1)
    val total: Long = buffer.getLong(0)

    //北京21.2%，天津13.2%，其他65.6%
    val top2CityCount: List[(String, Long)] = cityMap.toList.sortWith(_._2 > _._2).take(2)
    //计算百分比,并且使用样例类将结果保存起来，方便最后的展示
    val top2CityCountList: List[CityCountTop3] = top2CityCount.map { case (cityName, count) =>
      val cityRatio: Double = (count.toDouble / total * 1000 ) / 10.toDouble
      CityCountTop3(cityName, cityRatio)
    }
    //计算其他
    var otherRatio = 100D
    for (cityRatio <- top2CityCountList) {
      otherRatio -= cityRatio.cilckRaito
    }
    otherRatio = Math.round(otherRatio * 10) / 10.toDouble

    //将其他城市的百分比数据添加到list中
    val top3CityCount: List[CityCountTop3] = top2CityCountList :+ CityCountTop3("其他",otherRatio)

    //最后拼接成字符串
    top3CityCount.mkString(",")

  }
  case  class CityCountTop3 (cityName:String,cilckRaito:Double){
    override def toString: String = cityName + ":" + cilckRaito + "%"
  }

}
