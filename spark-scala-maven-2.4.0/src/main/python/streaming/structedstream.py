#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0") \
        .appName("pyspark_structured_streaming_kafka") \
        .getOrCreate()

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test") \
        .load()

    words = df.selectExpr("CAST(value AS STRING)")

    schema = StructType() \
        .add("name", StringType()) \
        .add("age", StringType()) \
        .add("sex", StringType())

    # 通过from_json，定义schema来解析json
    res = words.select(from_json("value", schema).alias("data")).select("data.*")

    query = res.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    query.awaitTermination()
