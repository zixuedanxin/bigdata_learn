package org.roy.demo.streaming.bus

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.{DataFrame, SparkSession}

@deprecated
object OrderJob {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("OrderJob")
      .getOrCreate()
    import spark.implicits._
    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "10.200.102.192")
      .option("port", 9998)
      .load().withColumn("current_timestamp", current_timestamp)
    // Split the lines into words
    //    val words = lines.as[String].flatMap(_.split(" ")) (String, Timestamp)
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
    // Without watermark using orderId column,?? I want to overwrite the old data here
    //    words.dropDuplicates("orderId").createOrReplaceTempView("tmp_table")
//    orderUpdates.createOrReplaceTempView("tmp_table")
//    val sql = "select storeId,otype, count(1) num,sum(money) as moneys  from tmp_table group by storeId,otype"
    //But it's not allowed. Is there any good suggestion?
    //    val sql =
    //      """
    //        |select tt1.storeId,tt1.otype, count(1) num,sum(money) as moneys  from (
    //        |select ROW_NUMBER() OVER (PARTITION BY orderId ORDER BY orderDate desc) as rn ,
    //        | t1.*  from  tmp_table as t1
    //        |) as tt1 where tt1.rn=1 group by storeId,otype
    //      """.stripMargin
//    val resultDF = spark.sql(sql)
    // Start running the query that prints the running counts to the console
    val query = orderUpdates.writeStream
      .outputMode("update")
      .format("console")
      .start()
    query.awaitTermination()

  }

}

case class orderEvent(orderId: String, otype: Int, storeId: String, money: Double, orderDate: String, timestamp: Timestamp)
