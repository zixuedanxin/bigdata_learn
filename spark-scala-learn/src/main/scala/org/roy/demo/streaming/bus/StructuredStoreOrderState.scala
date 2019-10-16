package org.roy.demo.streaming.bus

import java.sql.Timestamp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.streaming._
import streaming.StreamingExamples

/**
  * create by Roy 2019/09/06
  * Counting day order number and amount
  */
object StructuredStoreOrderState {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  StreamingExamples.setStreamingLogLevels()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local[*]")
      .appName("StructuredSessionization")
      .getOrCreate()
    import spark.implicits._
    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", "10.200.102.192")
      .option("port", 9998)
      .load().withColumn("current_timestamp", current_timestamp)


    //100,1,10,20,2019-09-03
    //1001,1,10,200,2019-09-03
    //1001,1,10,2000,2019-09-03
    val events = lines
      .as[(String, Timestamp)]
      .map { case (line, timestamp) => {
        val orderInfo = line.split(",")
        if (orderInfo != null && orderInfo.size > 4) {
          //分组key
          val gId = orderInfo(2) + "_" + orderInfo(1).toInt + "_" + orderInfo(4) //storeid+otype+orderdate
          val objEvent = dataEvent(gId, orderInfo(0), orderInfo(1).toInt, orderInfo(2), orderInfo(3).toDouble, orderInfo(4), timestamp)
          objEvent
        } else {
          null
        }
      }
      }.filter(obj => obj != null)
    val orderUpdates = events
      .groupByKey(event => event.gId)
      .mapGroupsWithState[storeOrderInfoState, storeOrderInfoStateUpdate](GroupStateTimeout.ProcessingTimeTimeout) {
      case (key: String, values: Iterator[dataEvent], state: GroupState[storeOrderInfoState]) =>
        val seqs = values.toSeq
        val times = seqs.map(_.timestamp).seq
        val max_time = new Timestamp(System.currentTimeMillis())
        if (state.hasTimedOut) {
          val finalUpdate =
            storeOrderInfoStateUpdate(key, state.get.orderNum, state.get.orderMoney, max_time, expired = true)
          state.remove()
          finalUpdate
        } else {
          val updatedSession = if (state.exists) {
            val stateMap = state.get.orderInfoStoreMap
            var norderMap2: Map[String, Double] = Map()
            var num = 0
            var money = 0.0
            seqs.foreach(e => {
              if (stateMap.contains(e.orderId)) {
                //新订单-旧订单，再进行+总合
                money += e.money - stateMap.get(e.orderId).get
              } else {
                num += 1
                money += e.money
              }
              norderMap2 += (e.orderId -> e.money)
            })
            //取出所有的订单+流进来的订单，需要判断是否有重复订单
            storeOrderInfoState(key, state.get.orderNum + num, state.get.orderMoney + money, stateMap ++ norderMap2, max_time)
          } else {
            var norderMap2: Map[String, Double] = Map()
            var money = 0.0
            seqs.foreach(e => {
              if (norderMap2.contains(e.orderId)) {
                money += e.money - norderMap2.get(e.orderId).get
              } else {
                money += e.money
              }
              norderMap2 += (e.orderId -> e.money)
            })
            storeOrderInfoState(key, norderMap2.size, money, norderMap2, max_time)
          }
          //更新缓存里面的这条数据信息
          state.update(updatedSession)
          // Set timeout such that the session will be expired if no data received for 10 seconds
          state.setTimeoutDuration("1 hour")
          storeOrderInfoStateUpdate(key, state.get.orderNum, state.get.orderMoney, max_time, expired = false)
        }
    }
    import org.apache.spark.sql.functions.udf
    import org.apache.spark.sql.functions.lit

    val storeIdCode = (gid: String, index: Int) => {
      gid.split("_")(index)
    }
    val storeidUDF = udf(storeIdCode)
    //    val otypeUDF = udf(storeIdCode)
    import spark.implicits._
    val resultDF = orderUpdates.withColumn("storeId", storeidUDF(orderUpdates("gId"), lit(0))).
      withColumn("otype", storeidUDF(orderUpdates("gId"), lit(1))).
      withColumn("orderDate", storeidUDF(orderUpdates("gId"), lit(2)))

    val checkpointLocation = "/scheckpoint/checkpoint-structuredStoreOrderState"
    // Delete the checkpoint location from previous executions，socket需要删除目录
    import java.nio.file.{Files, FileSystems}
    import java.util.Comparator
    import scala.collection.JavaConverters._
    val path = FileSystems.getDefault.getPath(checkpointLocation)
    //        .sorted(Comparator.reverseOrder())
    if (Files.exists(path)) {
      Files.walk(path)
        .iterator
        .asScala
        .foreach(p => p.toFile.delete)
    }

    val query = resultDF
      .writeStream
      .outputMode("update")
      .option("checkpointLocation", checkpointLocation) //user/spark/
      .format("console")
      .start()
    query.awaitTermination()
  }
}

/** User-defined data type representing the input events */
case class dataEvent(gId: String, orderId: String, otype: Int, storeId: String, money: Double, orderTime: String, timestamp: Timestamp)

//最一个订单状态
case class orderEnventInfo(orderId: String, otype: Int, storeId: String, money: Double, orderTime: String, timestamp: Timestamp)

//门店里面，维护一张所有订单
case class storeOrderInfoState(storeId: String, orderNum: Int, orderMoney: Double, orderInfoStoreMap: Map[String, Double], timestamp: Timestamp)

//返回计算后的结果
case class storeOrderInfoStateUpdate(gId: String, orderNum: Int, orderMoney: Double, timestamp: Timestamp,
                                     expired: Boolean)

