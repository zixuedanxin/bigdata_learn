package streamings.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import streamings.util.CSVFileStreamGenerator

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

object Basic01 {

  def main (args: Array[String]) {


   val fm = new CSVFileStreamGenerator(10, 5, 500)

    println("*** Starting to stream")

    val spark = SparkSession
      .builder
      .appName("StructuredStreaming_Basic")
      .config("spark.master", "local[4]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.SparkContext

    // schema for the streaming records
    val recordSchema = StructType(
      Seq(
        StructField("key", StringType),
        StructField("value", IntegerType)
      )
    )
//    val ds1 = spark.readStream.format("kafka")
//      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")//连接哪个服务器
//      .option("subscribe", "topic1,topic2")//也可("subscribePattern", "topic.*")
////      .load()
//    ds1.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")//不加"topic"就在后面加.option("topic", "topic1")
//      .writeStream.format("kafka")
//      .option("checkpointLocation", "/to/HDFS-compatible/dir")
//      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//      .start()
//    kafka的schema为key: binary；value: binary；topic: string；partition: int；offset: long；timestamp: long
//
//    读数据有三个选择：
//
//    assign：指定topic以及topic的某部分，如 JSON string{"topicA":[0,1],"topicB":[2,4]}
//
//    subscribe or subscribePattern：读取多个topics通过一列topics或一种模式（正则表达）
//
//    其他选项
//
//    startingOffsets and endingOffsets：earliest，latest或JSON string（{"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}，-1latest，-2earliest）。这仅适用于新的streaming，重新开始时会在结束时的位置开始定位。新发现的分区会在earliest开始，最后位置为查询的范围。
//
//    failOnDataLoss:a false alarm，默认true
//
//    maxOffsetsPerTrigger：每个触发间隔处理的最大偏移量的速率限制。指定的总偏移量将按不同卷的topicPartitions按比例分割。
//
//    还有一些Kafka consumer timeouts, fetch retries, and intervals.
    // a streaming DataFrame resulting from parsing the records of the CSV files
//    val csvDF = spark
//      .readStream
//      .option("sep", ",")
//      .schema(recordSchema)
//      .format("csv")
//      .load(fm.dest.getAbsolutePath)
//     println(fm.dest.getAbsolutePath)
//    // it has the schema we specified
//    csvDF.printSchema()

    // every time a batch of records is received, dump the new records
    // to the console -- often this will just eb the contents of a single file,
    // but sometimes it will contain mote than one file

    import  spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 3000)
      .load()
    // println(lines.)
    val words = lines.as[String].flatMap(_.split("\t").take(2))

    val wordCounts = words.groupBy("value").count()
    // wordCounts.show()
    val query = wordCounts.writeStream
      .queryName("activity_counts") // 类似tempview的表名，用于下面查询
      .outputMode("update") //complete：所有内容都输出  append：新增的行才输出  update：更新的行才输出
      .format("console") // debug专用  console memory
      .trigger(Trigger.ProcessingTime("10 seconds")) // 可选
      .start()// 批量输出用save

    query.awaitTermination()//防止driver在查询过程退出

    //执行上面的代码，stream已经运行。下面对output到内存中的table进行查询
    for( i <- 1 to 10 ) {
      Thread.sleep(10000)
      println(s"查询次数 ${i}")
      spark.sql("SELECT * FROM activity_counts").show()
    }

//    //所有select和filter都支持
//    val simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'"))
//      .where("stairs")
//      .where("gt is not null")
//      .select("gt", "model", "arrival_time", "creation_time")
//      .writeStream
//      .queryName("simple_transform")
//      .format("memory")
//      .outputMode("append")
//      .start()
//
//    //支持大部分Aggregations
//    val deviceModelStats = streaming.cube("gt", "model").avg()
//      .drop("avg(Arrival_time)")
//      .drop("avg(Creation_Time)")
//      .drop("avg(Index)")
//      .writeStream.queryName("device_counts").format("memory").outputMode("complete")
//      .start()
//    //限制在于multiple “chained” aggregations (aggregations on streaming aggregations) ，但可以通过output到sink后再aggregate来实现
//
//    //支持inner join，outer join要定义watermark
//    val historicalAgg = static.groupBy("gt", "model").avg()
//    val deviceModelStats = streaming
//      .drop("Arrival_Time", "Creation_Time", "Index")
//      .cube("gt", "model").avg()
//      .join(historicalAgg, Seq("gt", "model")) // inner join
//      .writeStream
//      .queryName("device_counts")
//      .format("memory")
//      .outputMode("complete")
//      .start()
    //Processing time trigger，如果Spark在100s内没有完成计算，Spark会等待下一个，而不是计算完成后马上计算下一个
    wordCounts.writeStream
      .trigger(Trigger.ProcessingTime("100 seconds"))//也可以用Duration in Scala or TimeUnit in Java
      .format("console")
      .outputMode("complete")
      .start()

    //Once trigger，测试中非常有用，常被用于低频工作（如向总结表格添加新数据）
//    wordCounts.writeStream
//      .trigger(Trigger.Once())
//      .format("console")
//      .outputMode("complete")
//      .start()
//    case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String,
//                      count: BigInt)
//    val dataSchema = spark.read
//      .parquet("/data/flight-data/parquet/2010-summary.parquet/")
//      .schema
//    val flightsDF = spark.readStream.schema(dataSchema)
//      .parquet("/data/flight-data/parquet/2010-summary.parquet/")
//    val flights = flightsDF.as[Flight]
//    def originIsDestination(flight_row: Flight): Boolean = {
//      return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
//    }
//    flights.filter(flight_row => originIsDestination(flight_row))
//      .groupByKey(x => x.DEST_COUNTRY_NAME).count()
//      .writeStream.queryName("device_counts").format("memory").outputMode("complete")
//      .start()

    // 基于event-time的window，words包含timestamp和word两列
//    wordCounts.withWatermark("timestamp", "30 minutes")//某窗口结果为x，但是部分数据在这个窗口的最后一个timestamp过后还没到达，Spark在这会等30min，过后就不再更新x了。
//      .dropDuplicates("User", "timestamp")
//      .groupBy(window(col("timestamp"), "10 minutes"),col("User"))// 10min后再加一个参数变为Sliding windows，表示每隔多久计算一次。
//      .count()
//      .writeStream
//      .queryName("events_per_window")
//      .format("memory")
//      .outputMode("complete")
//      .start()
//
//    spark.sql("SELECT * FROM events_per_window")
//

  }
}
