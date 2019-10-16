package org.roy.demo.streaming.structured

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.StreamingExamples
import org.apache.spark.sql.functions._

import scala.util.Random

object Deduplication2Test {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    StreamingExamples.setStreamingLogLevels()

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Deduplication2Test")
      .getOrCreate()

    // Read all the csv files written atomically in a directory
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val values = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema) // Specify schema of the csv files
      .csv("E:/user/spark/usertest") // Equivalent to format("csv").load("/path/to/directory")
    //    val values = spark.
    //      readStream.
    //      format("rate").
    //      load
    //      .withColumn("name", lit("xiaoming" + Random.nextInt(10)))
    //      .withColumn("age", lit(Random.nextInt(100)))
    values.printSchema()
    // Without watermark using guid column
    //    val newDF = values.dropDuplicates("name")
    import spark.implicits._
    values.createOrReplaceTempView("tmp_table")
    val resultDF = spark.sql("select name,count(*) from tmp_table group by name")
    val query = resultDF.
      writeStream.
      outputMode("complete").
      format("console").
      trigger(Trigger.ProcessingTime(10 * 1000)).
      start
    query.awaitTermination()
  }
}

case class User(name: String, age: Int)
