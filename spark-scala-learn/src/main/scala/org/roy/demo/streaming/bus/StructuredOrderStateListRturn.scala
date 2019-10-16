package org.roy.demo.streaming.bus

import java.sql.Timestamp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.streaming._
import streaming.StreamingExamples

@deprecated
object StructuredOrderStateListRturn {
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
          val objEvent = NOEvent(orderInfo(0), orderInfo(1).toInt, orderInfo(2), orderInfo(3).toDouble, orderInfo(4), timestamp)
          objEvent
        } else {
          null
        }
      }
      }.filter(obj => obj != null)
    /**
      * -次维护订单状态数据，返回最新的一个订单，并把上一个订单金额附加返回
      */
    val orderUpdates = events
      .groupByKey(event => event.orderId)
      //orderInfoStore=输入的状态类型，orderInfoStoreUpdate=输出的状态类型
      .mapGroupsWithState[orderInfoStore, orderInfoStoreUpdate](GroupStateTimeout.ProcessingTimeTimeout) {
      case (orderId: String, events: Iterator[NOEvent], state: GroupState[orderInfoStore]) =>
        // 如果时间超时，更新缓存
        if (state.hasTimedOut) {
          //时间过了，删除sotre里的数据，把expired=true,返回在线时间等信息 0:00-0:10可以设这个时间，不计算，干掉数据
          val finalUpdate =
            orderInfoStoreUpdate(orderId, state.get.otype, state.get.storeId, state.get.money, 0.0, state.get.orderDate, state.get.timestamp, expired = true)
          state.remove()
          finalUpdate
        } else {
          //订单没有超时，如果id存在，则替换掉，使用新的订单数据，或作别的操作
          var oldOrder = 0.0 //上一笔的金额
          val lastEnvent = events.toSeq.last
          val updatedSession = if (state.exists) {
            oldOrder = state.get.money
            //存在,算出旧的金额是多少
            orderInfoStore(orderId, lastEnvent.otype, lastEnvent.storeId, lastEnvent.money, oldOrder, lastEnvent.orderDate, lastEnvent.timestamp)
          } else {
            orderInfoStore(orderId, lastEnvent.otype, lastEnvent.storeId, lastEnvent.money, 0, lastEnvent.orderDate, lastEnvent.timestamp)
          }
          //更新缓存里面的这条数据信息
          state.update(updatedSession)
          // Set timeout such that the session will be expired if no data received for 10 seconds
          state.setTimeoutDuration("3600 seconds")
          orderInfoStoreUpdate(orderId, state.get.otype, state.get.storeId, state.get.money, oldOrder, state.get.orderDate, state.get.timestamp, expired = false)
        }
    }

    /**
      * 二次计算出门店的数据
      */
    val storeUpdate = orderUpdates.groupByKey(order => order.storeId).mapGroupsWithState[g1InfoStore, g1InfoStoreUpdate](GroupStateTimeout.ProcessingTimeTimeout) {
      case (storeId: String, events: Iterator[orderInfoStoreUpdate], state: GroupState[g1InfoStore]) =>
        // 如果时间超时，更新缓存
        if (state.hasTimedOut) {
          //时间过了，删除sotre里的数据，把expired=true,返回在线时间等信息 0:00-0:10可以设这个时间，不计算，干掉数据
          val finalUpdate =
            g1InfoStoreUpdate(storeId, state.get.num, state.get.money, state.get.timestamp, expired = true)
          state.remove()
          finalUpdate
        } else {
          var storeNum = events.map(_.orderId).size
          var storeMoney = events.map(_.money).reduce(_ + _) //其实只会一个
          val updatedStore = if (state.exists) { //门店存在，
            val old_order_moneys = events.map(_.oldMoney).reduce(_ + _) //其实只会一个
            //门店总客+=新订单金额-旧订单
            storeMoney = state.get.money + storeMoney - old_order_moneys
            g1InfoStore(storeId, state.get.num + events.map(_.orderId).size, storeMoney, state.get.timestamp)
          } else {
            g1InfoStore(storeId, storeNum, storeMoney, state.get.timestamp)
          }
          //更新缓存里面的这条数据信息
          state.update(updatedStore)
          // Set timeout such that the session will be expired if no data received for 10 seconds
          state.setTimeoutDuration("3600 seconds")
          g1InfoStoreUpdate(storeId, state.get.num, state.get.money, state.get.timestamp, expired = true)
        }
    }
    //门店统计好的数据在汇总
    //    storeUpdate.createOrReplaceTempView("update_tmp")
    //    spark.sql("select storeId,otype, count(1) num,sum(money) as moneys ,sum(oldMoney) as oldMoney  from update_tmp group by storeId,otype ")
    val query = storeUpdate
      .writeStream
      .outputMode("update")
      .format("console")
      .start()
    query.awaitTermination()
  }
}

/** User-defined data type representing the input events */
case class NOEvent(orderId: String, otype: Int, storeId: String, money: Double, orderDate: String, timestamp: Timestamp)

case class orderInfoStore(orderId: String, otype: Int, storeId: String, money: Double, oldMoney: Double, orderDate: String, timestamp: Timestamp)

case class orderInfoStoreUpdate(orderId: String, otype: Int, storeId: String, money: Double, oldMoney: Double, orderDate: String, timestamp: Timestamp,
                                expired: Boolean)


/** 第一个分组信息 */
case class g1InfoStore(storeid: String, /* otype: Int,*/ num: Int, money: Double, timestamp: Timestamp)

case class g1InfoStoreUpdate(storeid: String, /* otype: Int,*/ num: Int, money: Double, timestamp: Timestamp, expired: Boolean)

