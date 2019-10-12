package streaming.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import streaming.util.CSVFileStreamGenerator

/**
  * A very basic example of structured streaming as introduced in Spark 2.0.
  *
  * A sequence of CSV files is treated as a stream and subscribed to,
  * producing a streaming DataFrame.
  *
  * In this example, every time a batch of data is delivered the new records are
  * dumped to the console. Keep in mind that some batches will deliver only
  * one file, while others will deliver several files.
  */

object StructedStreamingJoin {
// https://www.cnblogs.com/code2one/p/9872355.html
  def main (args: Array[String]) {


   val fm = new CSVFileStreamGenerator(10, 5, 500)

    println("*** Starting to stream")

    val spark = SparkSession
      .builder
      .appName("StructuredStreaming_Basic")
      .config("spark.master", "local[4]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // schema for the streaming records
    val recordSchema = StructType(
      Seq(
        StructField("key", StringType),
        StructField("value", IntegerType)
      )
    )
//    Join
//    前提（inner join中可选，因为join没有交集可以去掉，但outer join必须设置）：
//
//    两个input都要定义watermark。
//    join条件：
//    Time range join conditions (e.g. ...JOIN ON leftTime BETWEN rightTime AND rightTime + INTERVAL 1 HOUR
//      Join on event-time windows (e.g. ...JOIN ON leftTimeWindow = rightTimeWindow)
//    支持：
//
//    stream-static：inner、left outer
//      stream-stream：inner、outer（必须设置wm和time constraints）
//    限制：
//
//    join的查询仅限append模式
//    join之前不能aggregation
//    update mode模式下join之前不能mapGroupsWithState and flatMapGroupsWithState
//
//    // 广告影响和点击量
//    val impressions = spark.readStream. ...
//    val clicks = spark.readStream. ...
//
//    // Apply watermarks on event-time columns
//    val impressionsWithWatermark = impressions.withWatermark("impressionTime", "2 hours")
//    val clicksWithWatermark = clicks.withWatermark("clickTime", "3 hours")
//
//    // Join with event-time constraints
//    impressionsWithWatermark.join(
//      clicksWithWatermark,
//      // a click can occur within a time range of 0 seconds to 1 hour after the corresponding impression
//      expr("""
//    clickAdId = impressionAdId AND
//    clickTime >= impressionTime AND
//    clickTime <= impressionTime + interval 1 hour
//    """),
//      joinType = ".." // "inner", "leftOuter", "rightOuter"
//    )

//    val sessionUpdates = events
//      .groupByKey(event => event.sessionId)
//      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {
//
//      case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>
//
//        // 处理值之前先检查time-out
//        if (state.hasTimedOut) {
//          val finalUpdate =
//            SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
//          state.remove()
//          finalUpdate
//        } else {
//          // 没有超时就先判断是否有相应的state，没有就创建，否则就更新。
//          val timestamps = events.map(_.timestamp.getTime).toSeq
//          val updatedSession = if (state.exists) {
//            val oldSession = state.get
//            SessionInfo(
//              oldSession.numEvents + timestamps.size,
//              oldSession.startTimestampMs,
//              math.max(oldSession.endTimestampMs, timestamps.max))
//          } else {
//            SessionInfo(timestamps.size, timestamps.min, timestamps.max)
//          }
//          state.update(updatedSession)
//
//          // 设置超时，以便在没有收到数据10秒的情况下会话将过期
//          state.setTimeoutDuration("10 seconds")
//          SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
//        }
//    }

//    //输入row的格式
//    case class InputRow(user:String, timestamp:java.sql.Timestamp, activity:String)
//    //表示user状态的格式(储存目前状态)
//    case class UserState(user:String,
//                         var activity:String,
//                         var start:java.sql.Timestamp,
//                         var end:java.sql.Timestamp)
//
//    //基于某row如何更新
//    def updateUserStateWithEvent(state:UserState, input:InputRow):UserState = {
//      if (Option(input.timestamp).isEmpty) {
//        return state
//      }
//      if (state.activity == input.activity) {
//
//        if (input.timestamp.after(state.end)) {
//          state.end = input.timestamp
//        }
//        if (input.timestamp.before(state.start)) {
//          state.start = input.timestamp
//        }
//      } else {
//        if (input.timestamp.after(state.end)) {
//          state.start = input.timestamp
//          state.end = input.timestamp
//          state.activity = input.activity
//        }
//      }
//      state
//    }
//
//    //基于一段时间的row如何更新
//    def updateAcrossEvents(user:String,
//                           inputs: Iterator[InputRow],
//                           oldState: GroupState[UserState]):UserState = {
//      var state:UserState = if (oldState.exists) oldState.get else UserState(user,
//        "",
//        new java.sql.Timestamp(6284160000000L),
//        new java.sql.Timestamp(6284160L)
//      )
//      // we simply specify an old date that we can compare against and
//      // immediately update based on the values in our data
//
//      for (input <- inputs) {
//        state = updateUserStateWithEvent(state, input)
//        oldState.update(state)
//      }
//      state
//    }
//
//    withEventTime
//      .selectExpr("User as user",
//        "cast(Creation_Time/1000000000 as timestamp) as timestamp", "gt as activity")
//      .as[InputRow]
//      .groupByKey(_.user)
//      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)
//      .writeStream
//      .queryName("events_per_window")
//      .format("memory")
//      .outputMode("update")
//      .start()

//    mapGroupsWithState例子：
//
//    基于user的activity（state）来更新。具体来说，如果收到的信息中，user的activity没变，就检查该信息是否早于或晚于目前所收到的信息，进而得出user某个activity的准确的持续时间（下面设置了GroupStateTimeout.NoTimeout，假设信息传输没丢失，则迟早会收到）。如果activity变了，就更新activity（时间也要重新设置）。如果没有activity，就不变。
//
//    //输入row的格式
//    case class InputRow(user:String, timestamp:java.sql.Timestamp, activity:String)
//    //表示user状态的格式(储存目前状态)
//    case class UserState(user:String,
//                         var activity:String,
//                         var start:java.sql.Timestamp,
//                         var end:java.sql.Timestamp)
//
//    //基于某row如何更新
//    def updateUserStateWithEvent(state:UserState, input:InputRow):UserState = {
//      if (Option(input.timestamp).isEmpty) {
//        return state
//      }
//      if (state.activity == input.activity) {
//
//        if (input.timestamp.after(state.end)) {
//          state.end = input.timestamp
//        }
//        if (input.timestamp.before(state.start)) {
//          state.start = input.timestamp
//        }
//      } else {
//        if (input.timestamp.after(state.end)) {
//          state.start = input.timestamp
//          state.end = input.timestamp
//          state.activity = input.activity
//        }
//      }
//      state
//    }
//
//    //基于一段时间的row如何更新
//    def updateAcrossEvents(user:String,
//                           inputs: Iterator[InputRow],
//                           oldState: GroupState[UserState]):UserState = {
//      var state:UserState = if (oldState.exists) oldState.get else UserState(user,
//        "",
//        new java.sql.Timestamp(6284160000000L),
//        new java.sql.Timestamp(6284160L)
//      )
//      // we simply specify an old date that we can compare against and
//      // immediately update based on the values in our data
//
//      for (input <- inputs) {
//        state = updateUserStateWithEvent(state, input)
//        oldState.update(state)
//      }
//      state
//    }
//
//    withEventTime
//      .selectExpr("User as user",
//        "cast(Creation_Time/1000000000 as timestamp) as timestamp", "gt as activity")
//      .as[InputRow]
//      .groupByKey(_.user)
//      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)
//      .writeStream
//      .queryName("events_per_window")
//      .format("memory")
//      .outputMode("update")
//      .start()

//    // 测试代码
//    val dir = new Path("/tmp/test-structured-streaming")
//    val fs = dir.getFileSystem(sc.hadoopConfiguration)
//    fs.mkdirs(dir)
//
//    val schema = StructType(StructField("vilue", StringType) ::
//      StructField("timestamp", TimestampType) ::
//      Nil)
//
//    val eventStream = spark
//      .readStream
//      .option("sep", ";")
//      .option("header", "false")
//      .schema(schema)
//      .csv(dir.toString)
//
//    // Watermarked aggregation
//    val eventsCount = eventStream
//      .withWatermark("timestamp", "1 hour")
//      .groupBy(window($"timestamp", "1 hour"))
//      .count
//
//    def writeFile(path: Path, data: String) {
//      val file = fs.create(path)
//      file.writeUTF(data)
//      file.close()
//    }
//
//    // Debug query
//    val query = eventsCount.writeStream
//      .format("console")
//      .outputMode("complete")
//      .option("truncate", "false")
//      .trigger(ProcessingTime("5 seconds"))
//      .start()
//
//    writeFile(new Path(dir, "file1"), """
//                                        |A;2017-08-09 10:00:00
//                                        |B;2017-08-09 10:10:00
//                                        |C;2017-08-09 10:20:00""".stripMargin)
//
//    query.processAllAvailable()
//    val lp1 = query.lastProgress
//
//    // -------------------------------------------
//    // Batch: 0
//    // -------------------------------------------
//    // +---------------------------------------------+-----+
//    // |window                                       |count|
//    // +---------------------------------------------+-----+
//    // |[2017-08-09 10:00:00.0,2017-08-09 11:00:00.0]|3    |
//    // +---------------------------------------------+-----+
//
//    // lp1: org.apache.spark.sql.streaming.StreamingQueryProgress =
//    // {
//    //   ...
//    //   "numInputRows" : 3,
//    //   "eventTime" : {
//    //     "avg" : "2017-08-09T10:10:00.000Z",
//    //     "max" : "2017-08-09T10:20:00.000Z",
//    //     "min" : "2017-08-09T10:00:00.000Z",
//    //     "watermark" : "1970-01-01T00:00:00.000Z"
//    //   },
//    //   ...
//    // }
//
//
//    writeFile(new Path(dir, "file2"), """
//                                        |Z;2017-08-09 20:00:00
//                                        |X;2017-08-09 12:00:00
//                                        |Y;2017-08-09 12:50:00""".stripMargin)
//
//    query.processAllAvailable()
//    val lp2 = query.lastProgress
//
//    // -------------------------------------------
//    // Batch: 1
//    // -------------------------------------------
//    // +---------------------------------------------+-----+
//    // |window                                       |count|
//    // +---------------------------------------------+-----+
//    // |[2017-08-09 10:00:00.0,2017-08-09 11:00:00.0]|3    |
//    // |[2017-08-09 12:00:00.0,2017-08-09 13:00:00.0]|2    |
//    // |[2017-08-09 20:00:00.0,2017-08-09 21:00:00.0]|1    |
//    // +---------------------------------------------+-----+
//
//    // lp2: org.apache.spark.sql.streaming.StreamingQueryProgress =
//    // {
//    //   ...
//    //   "numInputRows" : 3,
//    //   "eventTime" : {
//    //     "avg" : "2017-08-09T14:56:40.000Z",
//    //     "max" : "2017-08-09T20:00:00.000Z",
//    //     "min" : "2017-08-09T12:00:00.000Z",
//    //     "watermark" : "2017-08-09T09:20:00.000Z"
//    //   },
//    //   "stateOperators" : [ {
//    //     "numRowsTotal" : 3,
//    //     "numRowsUpdated" : 2
//    //   } ],
//    //   ...
//    // }
//
//    writeFile(new Path(dir, "file3"), "")
//
//    query.processAllAvailable()
//    val lp3 = query.lastProgress
//
//    // -------------------------------------------
//    // Batch: 2
//    // -------------------------------------------
//    // +---------------------------------------------+-----+
//    // |window                                       |count|
//    // +---------------------------------------------+-----+
//    // |[2017-08-09 10:00:00.0,2017-08-09 11:00:00.0]|3    |
//    // |[2017-08-09 12:00:00.0,2017-08-09 13:00:00.0]|2    |
//    // |[2017-08-09 20:00:00.0,2017-08-09 21:00:00.0]|1    |
//    // +---------------------------------------------+-----+
//
//    // lp3: org.apache.spark.sql.streaming.StreamingQueryProgress =
//    // {
//    //   ...
//    //   "numInputRows" : 0,
//    //   "eventTime" : {
//    //     "watermark" : "2017-08-09T19:00:00.000Z"
//    //   },
//    //   "stateOperators" : [ ],
//    //   ...
//    // }
//
//    query.stop()
//    fs.delete(dir, true)
//val conf = new SparkConf().setAppName(appName).setMaster(master)
//    val ssc = new StreamingContext(conf, Seconds(1))
//    val lines = ssc.socketTextStream/ textFileStream //等方法创造streams
//
//    val res = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
//
//    ssc.start()
//    ssc.awaitTermination()

  }
}
