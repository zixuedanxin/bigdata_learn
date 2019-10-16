package streaming.structured

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object StructuredTest {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()
    import spark.implicits._
    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "10.200.102.192")
      .option("port", 9998)
      .load()
    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))
    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
