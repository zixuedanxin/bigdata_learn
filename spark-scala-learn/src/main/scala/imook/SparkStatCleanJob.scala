package imook

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 第二次清洗
  */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {

    //拿到spark
    val spark = SparkSession.builder.appName("SparkStatCleanJob")
        .config("sprk.sql.parquet.compression.codec","gzip") //性能调优 压缩。因为默认为snappy，把改为gzip
        .master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file:///E:/myData/大数据开发项目/慕课日志spark/data/access.log")

         accessRDD.take(10).foreach(println)

        //把rdd转化为dataframe  RDD ==> DF
       val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtils.parsLog(x)),AccessConvertUtils.struct)
        accessDF.printSchema()
        accessDF.show(false)
        accessDF.write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day")
                .save("file:///E:/myData/大数据开发项目/慕课日志spark/data/clean/")

        spark.stop()
  }

}
