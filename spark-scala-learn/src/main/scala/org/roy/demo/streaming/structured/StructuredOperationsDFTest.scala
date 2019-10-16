package streaming.structured

import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StructuredOperationsDFTest {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  case class DeviceData(device: String, deviceType: String, signal: Double, time: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    // Read all the csv files written atomically in a directory
    val userSchema = new StructType().add("device", "string")
      .add("deviceType", "string")
      .add("signal", "double")
      .add("time", "long")


    import spark.implicits._
    val df = spark
      .readStream
      //      .option("sep", ";")
      .schema(userSchema)
      .csv("E:/user/spark/strucate/device.csv") // Equivalent to format("csv").load("/path/to/directory")
    df.printSchema()
    val ds: Dataset[DeviceData] = df.as[DeviceData]

    // Select the devices which have signal more than 10
    df.select("device").where("signal > 10") // using untyped APIs
    ds.filter(_.signal > 10).map(_.device) // using typed APIs

    // Running count of the number of updates for each device type
    val count = df.groupBy("deviceType").count() // using untyped API
    // Running average signal for each device type
    import org.apache.spark.sql.expressions.scalalang.typed
    ds.groupByKey(_.deviceType).agg(typed.avg(_.signal)) // using typed API

    println(count)

    df.createOrReplaceTempView("updates")
    val tabledf = spark.sql("select count(*) from updates") // returns another streaming DF
    tabledf.isStreaming
    println(tabledf)
  }

}
