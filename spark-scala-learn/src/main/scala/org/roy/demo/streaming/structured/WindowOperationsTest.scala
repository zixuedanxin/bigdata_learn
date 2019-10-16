package streaming.structured

import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import streaming.StreamingExamples
import org.apache.log4j.{Level, Logger}

object WindowOperationsTest {
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
      //      .option("includeTimestamp", true)
      .load()
      .withColumn("current_timestamp", current_timestamp)

    import spark.implicits._
    // Split the lines into words
    //    val words = lines.as[String].flatMap(_.split(" "))
    // Split the lines into words, retaining timestamps
    //    Calendar.getInstance().getTime
    //    val ts = new Timestamp(System.currentTimeMillis())
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")

    // Group the data by window and word and compute the count of each group
    //接这个数据的时间，5分钟内的，每1分钟进行分组，
    val windowedCounts = words.
      withWatermark("timestamp", "5 minutes") //这意味着系统需要知道旧的聚合何时可以从内存状态中删除，因为应用程序将不再接收该聚合的最新数据
      .groupBy(
      window($"timestamp", "5 minutes", "1 minutes"),
      $"word"
    ).count()

    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
