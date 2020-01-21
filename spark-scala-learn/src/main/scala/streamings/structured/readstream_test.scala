package streamings.structured

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.{ForeachWriter, Row, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import streamings.util.CSVFileStreamGenerator


object readstream_test {

  def main (args: Array[String]) {


    val spark = SparkSession
      .builder
      .appName("StructuredStreaming_Basic")
      .config("spark.master", "local[4]")
      .config("spark.sql.shuffle.partitions",10) // 对于sql\dataset\dataframe等设置task数量 200 2-3秒，20个0.3-0.6秒 10个 0.1-0.2
      .config("spark.default.parallelism",10)  // 对于非sql 等的task数据
      .config("spark.driver.memory","100M")  // driver 内存
      .config("spark.executor.memory","512M") // 至少512m 执行内存
      //.config("spark.storage.memory","800M") // storage内存
      .config("spark.storage.memoryFraction",0.2) // 至少512m storage内存占executor的内存占比
      .config("spark.yarn.executor.memoryOverhead","512M") // 堆外内存设置
      .config("spark.shuffle.memoryFraction",0.4)
      .getOrCreate()
    /*
    Spark静态内存管理机制，堆内存被划分为了两块，Storage和Execution。
    Storage主要用于缓存RDD数据和broadcast数据，
    Execution主要用于缓存在shuffle过程中产生的中间数据，Storage占系统内存的60%，Execution占系统内存的20%，并且两者完全独立。
    Spark统一内存管理机制，堆内存被划分为了两块，Storage和Execution。
    Storage主要用于缓存数据，Execution主要用于缓存在shuffle过程中产生的中间数据，
    两者所组成的内存部分称为统一内存，Storage和Execution各占统一内存的50%，由于动态占用机制的实现，
    shuffle过程需要的内存过大时，会自动占用Storage的内存区域，因此无需手动进行调节

    堆外内存
    spark.yarn.executor.memoryOverhead
     */
    spark.sparkContext.setLogLevel("WARN")

//    val url="jdbc:mysql://10.12.5.37:3306/bi"
//    val user ="dps"
//    val pwd = "dps1234"
//    val writer = new JDBCSink(url,user, pwd)
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 3000)
      .load()
    // println(lines.)
    println(lines.getClass)
    val words = lines.toDF("value").select($"value".cast("string"))
      .withColumn("_tmp", split($"value", "\t"))
      .select(
        $"_tmp".getItem(0).as("word"),
        $"_tmp".getItem(4).as("time")
      )
      .drop("_tmp")
      .withColumn("post_time", to_timestamp($"time", "yyyy-MM-dd HH:mm:ss"))
      .drop("time")
      .withWatermark("post_time", "10 minutes")
      .groupBy(
        window($"post_time", "5 minutes", "5 minutes"),
        $"word"
      ).count()


    //as[String] //.flatMap(_.split("\t")).select()
    println(words.getClass)
    println(words.printSchema())
//    val wordCounts=words
//      .withWatermark("post_time", "10 minutes")
//      .groupBy(
//        window($"post_time", "5 minutes", "5 minutes"),
//        $"word"
//      ).count()
    //val wordCounts = words.groupBy("value").count()

    /*
    https://blog.csdn.net/l_15156024189/article/details/81612860
    https://blog.csdn.net/lovebyz/article/details/75045514

    complete
    1、对于group(聚合) 输出的是各个时间仓口的汇总数据，对各个时间仓口数据汇总就是所有数据的汇总（历史时间窗口都在）
    2、对非聚合数据,complete 不支持
    3、没有时间窗口的，全量汇总输出
    update:
    1、对于聚合操作数据，会把最新聚合结果且数据有变化的输出(也是按某个时间窗口的，删除老的时间窗口，可以认为是对complete的增量处理)
    2、对于非聚合数据,相当于增量输出明细,等于append 如果程序中没有使用聚合，使用Append
    3、没有窗口时间的，把有变化的数据输出
    append
    1、对于聚合数据，不支持
    2、对于非聚合数据,相当于增量输出明细,等于update 如果程序中没有使用聚合，使用Append
     */
    val query = words.writeStream
      //.queryName("activity_counts") // 类似tempview的表名，用于下面查询
      .outputMode("update") //complete：所有内容都输出  append：新增的行才输出  update：更新的行才输出
      .format("console") // debug专用  console memory
      .option("truncate", "false")
     .trigger(Trigger.ProcessingTime("60 seconds")) // 可选 如果不设置任何触发器,将默认使用此触发器.触发规则是:上一个微批处理结束,启动下一个微批处理
      .start()// 批量输出用save
      query.awaitTermination()//防止driver在查询过程退出

  }
}

//    jdbcDF.write.format("jdbc").option("url","jdbc:mysql://mysql.com:3306/test")
//    Data source jdbc does not support streamed writing
//      .option("dbtable","test.test")
//      .option("user","admin")
//      .option("password","000000")
//      .mode(SaveMode.Overwrite) //如果存在表就重写数据
//      .save()
//    val words2 =lines.as[String].flatMap(x=>x.split(" "))
//  .toDF("word")
//  .groupBy("word").count()
//    val test = words2.select($"word",$"count"+1)
//    val query1 = words2.writeStream
//      .outputMode("update")
//      .format("console")
//      .option("truncate","false")
//      .start()
//    val query2 =test.writeStream
//      .outputMode("update") .format("console") .start()
//    query1.awaitTermination()
//    query2.awaitTermination()





/*
目前已知的一个替代方法是使用Dataset的createTempView方法来创建一个临时视图，
然后分支操作,即分支Query都基于这个临时视图展开。要实现这个目标，还做一个重要的动作：
不能使用Query的awaitTermination，因为之会阻塞第二个分支Query的执行，而应该在所有的Query执行完start之后，
使用sparkSession.streams.awaitAnyTermination()，只有这样才能确保两个分支Query都能启动。
https://www.cnblogs.com/NightPxy/p/9278881.html
https://blog.csdn.net/lovebyz/article/details/75045514
 */
class  JDBCSink(url:String, user:String, pwd:String) extends ForeachWriter[Row]{
  val driver = "com.mysql.jdbc.Driver"
  var connection:Connection = _
  var statement:Statement = _

  def open(partitionId: Long,version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }

  def process(value: Row): Unit = {
    statement.executeUpdate("INSERT INTO bill_details " +
      "VALUES (" + value.getAs("id") + ",'" + value.getAs("firstName") + "','" + value.getAs("lastName") + "'," +
      value.getAs("dataUsage") + "," + value.getAs("minutes") + "," + value.getAs("billAmount")+ ")")
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }

}