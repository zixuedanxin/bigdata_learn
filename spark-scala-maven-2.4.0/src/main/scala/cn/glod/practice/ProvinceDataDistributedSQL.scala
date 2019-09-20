package cn.glod.practice

import cn.glod.config.ConfigHelper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhangjin
  * @create 2018-06-29 21:07
  */
object ProvinceDataDistributedSQL {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName("各省市的数据分布统计")
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sparkContext = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sparkContext)

    // 读取数据 parquet
    val dataFrame = sqlContext.read.parquet(ConfigHelper.parquetPath)

    // 注册临时表
    dataFrame.registerTempTable("log")


    // 写sql统计数据
    val analysisResult: DataFrame = sqlContext.sql(
      """
        |select provincename, cityname, count(if(requestmode==1 && processnode >= 0,true,null) ) as defaultReq
        |from log group by provincename, cityname, to_date(requestdate)
      """.stripMargin)





    // 输出的目录已经存在，如何删除呢？ 只能在local模式下这样使用，在集群环境下不能这样用，用这里面的路径是一个本地路径，不代表集群中每台节点都有这个目录
    /*val file = new File("F:/dmp_32/json")
    if (file.exists()) {
        // file.delete()
        FileUtils.deleteDirectory(file)
    }*/

    // fileSystem ? 本地还是集群呢？ 客户端参数fs.defaultFS=file:///
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)
    val path = new Path("/Users/zhangjin/myCode/learn/spark-dmp/output/data1")
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }


    // 将结果数据写成json文件格式
    analysisResult
      .coalesce(4)
      .write
      .json("/Users/zhangjin/myCode/learn/spark-dmp/output/data1")

    // 将结果写入到数据库
    //    analysisResult.write.mode(SaveMode.Append).jdbc(ConfigHepler.dbUrl, "province_city_analysis_32", ConfigHepler.dbProps)


    sparkContext.stop()

  }

}
