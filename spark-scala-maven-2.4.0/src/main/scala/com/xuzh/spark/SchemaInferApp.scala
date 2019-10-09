package com.xuzh.spark

import org.apache.spark.sql.SparkSession

/**
 * Schema Infer
 */
object SchemaInferApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SchemaInferApp").master("local[2]").getOrCreate()

    val df = spark.read.format("json").load("file:///Users/rocky/data/json_schema_infer.json")

    df.printSchema()

    df.show()

    spark.stop()
  }

}
