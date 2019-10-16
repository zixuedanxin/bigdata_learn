package org.roy.demo.streaming.structured

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.window
import streaming.StreamingExamples

object DeduplicationTest {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    StreamingExamples.setStreamingLogLevels()

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("WindowOperationsTest")
      .getOrCreate()

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "10.200.102.192")
      .option("port", 9998)
      .option("includeTimestamp", true)
      .load()
    import spark.implicits._
    // Split the lines into words
    //    val words = lines.as[String].flatMap(_.split(" "))
    // Split the lines into words, retaining timestamps
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")


    // Without watermark using guid column  全部干掉重复的
    words.dropDuplicates("guid")

    // With watermark using guid and eventTime columns  在这个时间范围里，干掉重复的
    words
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicates("guid", "eventTime")

    val query = words.writeStream
      .outputMode("complete")
      .format("console")
      .start()


//    // Group the data by window and word and compute the count of each group
//    val windowedCounts = words.
//      //我们引入了水印，它允许引擎自动跟踪数据中的当前事件时间，并尝试相应地清理旧状态
//      withWatermark("timestamp", "10 minutes").
//      groupBy(
//        window($"timestamp", "10 minutes", "5 minutes"),
//        $"word"
//      ).count()


    query.awaitTermination()
  }
}
