package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
 * Parquet文件操作
 */
object ParquetApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkSessionApp")
      .master("local[2]").getOrCreate()


    /**
     * spark.read.format("parquet").load 这是标准写法
     */
    val userDF = spark.read.format("parquet").load("file:///home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet")

    userDF.printSchema()
    userDF.show()

    userDF.select("name","favorite_color").show

    userDF.select("name","favorite_color").write.format("json").save("file:///home/hadoop/tmp/jsonout")

    spark.read.load("file:///home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet").show

    //会报错，因为sparksql默认处理的format就是parquet
    spark.read.load("file:///home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").show

    spark.read.format("parquet").option("path","file:///home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet").load().show
    spark.stop()
  }

}
