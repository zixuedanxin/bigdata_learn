package imook

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * TopN统计spark作业
  */
object TopNStartJob {

  def main(args: Array[String]): Unit = {
    //拿到spark
    val spark = SparkSession.builder.appName("TopNStartJob")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled","false") //数据类型调整
        .master("local[2]").getOrCreate()
                                        //第二次清洗后的数据
    val accessDF = spark.read.format("parquet").load("file:///E:/myData/大数据开发项目/慕课日志spark/data/clean/")


    val day = "20170511"
    StatDAO.deleteData(day)

    //最受欢迎的TopN课程
    viedeoAccessTopNStart(spark, accessDF,day)

    //按照地市进行统计TopN课程
    cityAccessTopNStart(spark, accessDF,day);
    //按流量进行统计TopN课程
    videoTrafficsTopNStat(spark, accessDF,day)
    spark.stop()
  }


  /**
  * 按照流量进行统计TopN课程
    */

  def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame, day:String): Unit = {
    import spark.implicits._   //隐式转换
    val trafficAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
    .groupBy("day","cmsId").agg(sum("traffic").as("traffics"))
        .orderBy($"traffics".desc)
        //.show(false)

    /**
      * 将统计结果写入到mysql中
      *
      */
    try{
      trafficAccessTopNDF.foreachPartition(partitionOfRecords =>{

        val list = new ListBuffer[DayVideoTrafficsStat]

        partitionOfRecords.foreach(info =>{

          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")

          list.append(DayVideoTrafficsStat(day,cmsId,traffics))

        })

        StatDAO.insertDayVideoTrafficsAccessTopN(list)

      })
    }catch {

      case e: Exception => e.printStackTrace()

    }
  }

  /**
    * 按照地市进行统计TopN的课程
    * @param spark
    * @param accessDF
    */
  def cityAccessTopNStart(spark:SparkSession, accessDF:DataFrame, day:String):Unit = {
    //方式1：采用DataFrame API完成统计分析
    import spark.implicits._//隐式转换
    val cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day","city","cmsId").agg(count("cmsId").as("times"))  //  agg累加
     //cityAccessTopNDF.show(false)
    //Window函数在Spark SQLd的使用

    val top3DF = cityAccessTopNDF.select(cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc)
      ).as("times_rank")
      ).filter("times_rank <= 3")//.show(false)  //统计每个地市最受欢迎的3个Top3


    /**
      * 将统计结果写入到mysql中
      *
      */
    try{
      top3DF.foreachPartition(partitionOfRecords =>{

        val list = new ListBuffer[DayCityVideoAccessStat]

        partitionOfRecords.foreach(info =>{

          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(day,cmsId, city, times, timesRank))

        })

        StatDAO.insertDayCityVideoAccessTopN(list)

      })
    }catch {

      case e: Exception => e.printStackTrace()

     }

  }

  /**
    * 最受欢迎的TopNf课程
    * @param spark
    * @param accessDF
    * @return
    */
  def viedeoAccessTopNStart(spark: SparkSession, accessDF:DataFrame, day:String): Unit = {

    //方式1：采用DataFrame API完成统计分析
    import spark.implicits._//隐式转换
      val videoAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
               .groupBy("day","cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)  //按照times字段降序  agg累加

      videoAccessTopNDF.show(false)

    //方式2：采用SQL API完成统计分析
    /*accessDF.createOrReplaceTempView("access_logs")
    val videoAccessTopDNF = spark.sql("select day,cmsId, count(1) as times from access_logs"+
    "where day = '20170511' and cmsType = 'video'"+ "group by day, cmsId order by times desc")*/

    /**
      * 将统计结果写入到mysql中
      *
      */
    try{
      videoAccessTopNDF.foreachPartition(partitionOfRecords =>{

        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info =>{

          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day,cmsId, times))

        })

        StatDAO.insertDayVideoAccessTopN(list)

      })
    }catch {

      case e: Exception => e.printStackTrace()

    }

  }

}
