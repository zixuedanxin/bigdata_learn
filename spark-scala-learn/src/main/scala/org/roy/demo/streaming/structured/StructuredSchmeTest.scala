package streaming.structured

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object StructuredSchmeTest {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredSchmeTest")
      .getOrCreate()


    import spark.implicits._
    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val socketDF = spark.readStream
      .format("socket")
      .option("host", "10.200.102.194")
      .option("port", 9998)
      .load()

    socketDF.isStreaming // Returns True for DataFrames that have streaming sources
    socketDF.printSchema


    // Read all the csv files written atomically in a directory
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema) // Specify schema of the csv files
      .csv("E:/user/spark/strucate_test.csv") // Equivalent to format("csv").load("/path/to/directory")
    csvDF.printSchema()


  }

}
