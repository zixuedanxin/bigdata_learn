package com.hy.spark
import java.io.FileInputStream
import java.util.Properties
import com.mongodb.spark.MongoSpark
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.bson.{BsonArray, BsonDocument, Document}

object test {
  def main(args: Array[String]): Unit = {


  //  val error_json = BSONDocument("error" -> "errors")
    println("error_json")

  }
}
