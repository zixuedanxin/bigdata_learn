package streaming.structured

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
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

object Basic {

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

    // a streaming DataFrame resulting from parsing the records of the CSV files
    val csvDF = spark
      .readStream
      .option("sep", ",")
      .schema(recordSchema)
      .format("csv")
      .load(fm.dest.getAbsolutePath)
     println(fm.dest.getAbsolutePath)
    // it has the schema we specified
    csvDF.printSchema()

    // every time a batch of records is received, dump the new records
    // to the console -- often this will just eb the contents of a single file,
    // but sometimes it will contain mote than one file
    val query = csvDF.writeStream
      .outputMode("append")
      .format("console")
      .start()

    println("*** done setting up streaming")

    Thread.sleep(5000)

    println("*** now generating data")
    fm.makeFiles()

    Thread.sleep(5000)

    println("*** Stopping stream")
    query.stop()

    query.awaitTermination()
    println("*** Streaming terminated")

    println("*** done")
  }
}

/*
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

/**
  * 结构化流从kafka中读取数据存储到关系型数据库mysql
  * 目前结构化流对kafka的要求版本0.10及以上
  */
object StructuredStreamingKafka {

  case class Weblog(datatime: String,
                    userid: String,
                    searchname: String,
                    retorder: String,
                    cliorder: String,
                    cliurl: String)

  val LOGGER: Logger = LogManager.getLogger("vita")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("streaming")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop01:9092")
      .option("subscribe", "webCount")
      .load()
//        val df = spark.readStream
//          .format("socket")
//          .option("host", "localhost")
//          .option("port", 9998)
//          .load()

    import spark.implicits._

    val lines = df.selectExpr("CAST(value AS STRING)").as[String]
    //    //    lines.map(_.split(",")).foreach(x => print(" 0 = " + x(0) + " 1 = " + x(1) + " 2 = " + x(2) + " 3 = " + x(3) + " 4 = " + x(4) + " 5 = " + x(5)))

    val weblog = lines.map(_.split(","))
      .map(x => Weblog(x(0), x(1), x(2), x(3), x(4), x(5)))

    val titleCount = weblog
      .groupBy("searchname")
      .count()
      .toDF("titleName", "count")

    val url = "jdbc:mysql://hadoop01:3306/test"
    val username = "root"
    val password = "root"

    val writer = new JDBCSink(url, username, password)
    //        val writer = new MysqlSink(url, username, password)

    val query = titleCount
      .writeStream
      .foreach(writer)
      .outputMode("update")
      .trigger(ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
  }
}
 */