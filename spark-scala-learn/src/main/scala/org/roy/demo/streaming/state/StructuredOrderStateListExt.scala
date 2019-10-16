/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.roy.demo.streaming.state

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.streaming._
import streaming.StreamingExamples


/**
  * Counts words in UTF8 encoded, '\n' delimited text received from the network.
  *
  * Usage: MapGroupsWithState <hostname> <port>
  * <hostname> and <port> describe the TCP server that Structured Streaming
  * would connect to receive data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  * `$ nc -lk 9999`
  * and then run the example
  * `$ bin/run-example sql.streaming.StructuredSessionization
  * localhost 9999`
  */
object StructuredOrderStateListExt {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  StreamingExamples.setStreamingLogLevels()

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: StructuredSessionization <hostname> <port>")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt

    val spark = SparkSession
      .builder.master("local[*]")
      .appName("StructuredSessionization")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      //      .option("includeTimestamp", true)
      .load().withColumn("current_timestamp", current_timestamp)
    // Split the lines into words, treat words as sessionId of events
    val events = lines
      .as[(String, Timestamp)]
      .map { case (line, timestamp) => {
        val orderInfo = line.split(",")
        if (orderInfo != null && orderInfo.size > 4) {
          val objEvent = NOEvent(orderInfo(0), orderInfo(1).toInt, orderInfo(2), orderInfo(3).toDouble, orderInfo(4), timestamp)
          val sysDate = new SimpleDateFormat("yyyy-MM-dd").format(objEvent.timestamp)
          println("sysDate==" + sysDate)
          println("sysDate==" + sysDate.equals(objEvent.orderDate))
          if (sysDate.equals(objEvent.orderDate)) {
            objEvent
          } else {
            null
          }
        } else {
          null
        }
      }
      }.filter(obj => obj != null)
    //    val events = lines
    //      .as[(String, Timestamp)]
    //      .map { case (line, timestamp) => {
    //        val orderInfo = line.split(",")
    //        OEvent(orderInfo(0), orderInfo(1).toInt, orderInfo(2), orderInfo(3).toDouble, orderInfo(4), timestamp)
    //      }
    //      }.filter(obj => {
    //      val sysDate = new SimpleDateFormat("yyyy-MM-dd").format(obj.timestamp)
    //      println("sysDate==" + sysDate)
    //      println("sysDate==" + sysDate.equals(obj.orderDate))
    //      sysDate.equals(obj.orderDate)
    //    })
    //
    //    val sessionUpdates2 = events.groupByKey(event => event.storeId).count()

    //     Sessionize the events. Track number of events, start and end timestamps of session, and
    //     and report session updates.
    val sessionUpdates = events
      .groupByKey(event => event.storeId)
      .mapGroupsWithState[orderInfoStore, orderInfoStoreUpdate](GroupStateTimeout.ProcessingTimeTimeout) {
      case (storeId: String, events: Iterator[NOEvent], state: GroupState[orderInfoStore]) =>
        println("events==" + events)
        // 如果时间超时，更新缓存
        if (state.hasTimedOut) {
          println("超时:" + state.hasTimedOut)
          //时间过了，删除sotre里的数据，把expired=true,返回在线时间等信息
          val finalUpdate =
            orderInfoStoreUpdate(storeId, state.get.otype, state.get.storeId, state.get.money, state.get.orderDate, state.get.timestamp, expired = true)
          state.remove()
          finalUpdate
        } else {
          //订单没有超时，如果id存在，则替换掉，使用新的订单数据，或作别的操作
          println("更新缓存")
          // Update start and end timestamps in session 更新缓存
          val timestamps = events.map(_.timestamp.getTime).toSeq //数量累加
          val updatedSession = if (state.exists) {
            //取时间最大的订单id
            events
            state.get
          } else {
            events
            state.get
          }
          //更新缓存里面的这条数据信息
          state.update(updatedSession)
          // Set timeout such that the session will be expired if no data received for 10 seconds
          state.setTimeoutDuration("10 seconds")
          orderInfoStoreUpdate(storeId, state.get.otype, state.get.storeId, state.get.money, state.get.orderDate, state.get.timestamp, expired = false)
        }
    }

    // Star
    //|   id|durationMs|numEvents|expired|
    //+-----+----------+---------+-------+
    //|hello|         0|        1|  false|
    //+-----+----------+---------+-------+
    //
    //-------------------------------------------
    //Batch: 11
    //-------------------------------------------t running the query that prints the session updates to the console
    val query = sessionUpdates
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }

}

/** User-defined data type representing the input events */
case class NOEvent(orderId: String, otype: Int, storeId: String, money: Double, orderDate: String, timestamp: Timestamp)

/**
  * User-defined data type for storing a session information as state in mapGroupsWithState.
  *
  * @param numEvents        total number of events received in the session
  * @param startTimestampMs timestamp of first event received in the session when it started
  * @param endTimestampMs   timestamp of last event received in the session before it expired
  */
//case class orderAggInfo(
//                         num: Int,
//                         money: Double,
//                         startTimestampMs: Long,
//                         endTimestampMs: Long) {
//  /** Duration of the session, between the first and last events */
//  def durationMs: Long = endTimestampMs - startTimestampMs
//}
//sotre里的订单，如果有重复，就更新它
case class orderInfoStore(orderId: String, otype: Int, storeId: String, money: Double, orderDate: String, timestamp: Timestamp)


/**
  * User-defined data type representing the update information returned by mapGroupsWithState.
  *
  * @param id         Id of the session
  * @param durationMs Duration the session was active, that is, from first event to its expiry
  * @param numEvents  Number of events received by the session while it was active
  * @param expired    Is the session active or expired
  */
case class orderInfoStoreUpdate(orderId: String, otype: Int, storeId: String, money: Double, orderDate: String, timestamp: Timestamp,
                                expired: Boolean)

// scalastyle:on println
