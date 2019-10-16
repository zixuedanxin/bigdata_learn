package com.struct.stream

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StructField, StructType, _}

object SparkStructKafkaApp extends InitSpark {

  def main(args: Array[String]) = {

    val version = spark.version
    println("SPARK VERSION = " + version)

    val kafkaParams = Map[String, String](
      "kafka.bootstrap.servers" -> "scdf-release-kafka-headless:9092",
      "key.deserializer" -> " org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> " org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "structured-kafka","startingOffsets"-> "earliest" , "maxOffsetsPerTrigger" -> "20" ,
      "failOnDataLoss" -> "false")

    val schema = StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("firstName", StringType, true),
      StructField("lastName", StringType, true),
      StructField("dataUsage", LongType, true),
      StructField("minutes", LongType, true),
      StructField("billAmount", DoubleType, true)

      /*StructField("id", StringType, true),
      StructField("name", StringType, true),
      StructField("gender", StringType, true),
      StructField("cls", StringType, true),
      StructField("seat", StringType, true),
      StructField("club", StringType, true),
      StructField("persona", StringType, true),
      StructField("crush", StringType, true),
      StructField("breastSize", StringType, true),
      StructField("strength", StringType, true),
      StructField("hairstyle", StringType, true),
      StructField("color", StringType, true),
      StructField("eyes", StringType, true),
      StructField("eyeType", StringType, true),
      StructField("stockings", StringType, true),
      StructField("accessory", StringType, true),
      StructField("scheduleTime", StringType, true),
      StructField("scheduleDestination", StringType, true),
      StructField("scheduleAction", StringType, true),
      StructField("info", StringType, true)*/
    ))

    val data = spark.
      readStream.
      format("kafka").
      option("subscribe", "bill_details").
      options(kafkaParams).
      load().selectExpr("CAST(value AS STRING)")

    val billDF = data.select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*")

   /* billDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()*/

    val url="jdbc:mysql://scdf-release-mysql:3306/dataflow"
    val user ="root"
    val pwd = "cwLAxh0JdN"

    val writer = new JDBCSink(url,user, pwd)

    billDF.writeStream
      .foreach(writer)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("25 seconds"))
      .start()
      .awaitTermination()


  }
}
