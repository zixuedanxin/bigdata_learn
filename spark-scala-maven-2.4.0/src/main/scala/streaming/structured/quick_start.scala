package streaming.structured
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object quick_start {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local[2]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()
    import spark.implicits._
    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 3000)
      .load()

    // Split the lines into words
    val words = lines.as[String].map(_.split("\t")(0))
    // words.foreach(x=>println(x))
    // Generate running word count
    //println(words.collect())
    val wordCounts = words.groupBy("value").count()
    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
