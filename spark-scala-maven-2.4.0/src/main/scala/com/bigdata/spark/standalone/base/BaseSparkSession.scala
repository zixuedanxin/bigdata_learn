package com.bigdata.spark.standalone.base

import java.io.File

// import com.opensource.bigdata.spark.sql.n_02_spark_dataset.action.n_15_dataset_show_truncate_vertical.Run.sparkSession
import org.apache.spark.sql.SparkSession

/**
  * 得到SparkSession
  * 首先 extends BaseSparkSession
  * 本地: val spark = sparkSession(true)
  * 集群:  val spark = sparkSession()
  */
class BaseSparkSession {

  var appName = "sparkSession"
  var master = "local"// "spark://standalone.com:7077" //本地模式:local     standalone:spark://master:7077


  def sparkSession(): SparkSession = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession.builder
      .master(master)
      .appName(appName)
      .config("spark.sql.warehouse.dir",warehouseLocation)
      //.config("spark.eventLog.enabled","true")
      //.config("spark.history.fs.logDirectory","hdfs://standalone.com:9000/spark/log/historyEventLog")
     // .config("spark.eventLog.dir","hdfs://standalone.com:9000/spark/log/historyEventLog")
      .getOrCreate()
    //spark.sparkContext.addJar("/opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/target/spark-scala-maven-2.4.0-1.0-SNAPSHOT.jar")
    //import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  /**
    *
    * @param isLocal
    * @param isHiveSupport
    * @param remoteDebug
    * @param maxPartitionBytes  -1 不设置，否则设置分片大小
    * @return
    */

  def sparkSession(isLocal:Boolean = false, isHiveSupport:Boolean = false, remoteDebug:Boolean=false,maxPartitionBytes:Int = -1,numPartitions:Int =200): SparkSession = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    if(isLocal){
      master = "local[4]"
      var builder = SparkSession.builder
        .master(master)
        .appName(appName)
        .config("spark.sql.warehouse.dir",warehouseLocation)

      if(isHiveSupport){
        builder = builder.enableHiveSupport()
          //.config("spark.sql.hive.metastore.version","2.3.3")
      }

      //调置分区大小(分区文件块大小)
      if(maxPartitionBytes != -1){
        builder.config("spark.sql.files.maxPartitionBytes",maxPartitionBytes) //32
      }

     //The default number of partitions to use when shuffling data for joins or aggregations.
      if(numPartitions != 200){
        builder.config("spark.sql.shuffle.partitions",numPartitions)
      }

      builder.config("spark.executor.heartbeatInterval","10000s") //心跳间隔，超时设置
      builder.config("spark.network.timeout","100000s") //网络间隔，超时设置

      val spark = builder.getOrCreate()

      //spark.sparkContext.addJar("/opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/target/spark-scala-maven-2.4.0-1.0-SNAPSHOT.jar")
      //import spark.implicits._
      spark.sparkContext.setLogLevel("WARN")
      spark
    }else{

      var builder = SparkSession.builder
        .master(master)
        .appName(appName)
        .config("spark.sql.warehouse.dir",warehouseLocation)
        //.config("spark.eventLog.enabled","true")
       // .config("spark.eventLog.compress","true")
        //.config("spark.history.fs.logDirectory","hdfs://standalone.com:9000/spark/log/historyEventLog")
        //.config("spark.eventLog.dir","hdfs://standalone.com:9000/spark/log/historyEventLog")

        //调置分区大小(分区文件块大小)
        if(maxPartitionBytes != -1){
          builder.config("spark.sql.files.maxPartitionBytes",maxPartitionBytes) //32
        }



       // .config("spark.sql.shuffle.partitions",2)

       //executor debug,是在提交作的地方读取
        if(remoteDebug){

          builder.config("spark.executor.extraJavaOptions","-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=10002")

          builder.config("spark.executor.heartbeatInterval","10000s") //心跳间隔，超时设置
          builder.config("spark.network.timeout","100000s") //网络间隔，超时设置
        }



      if(isHiveSupport){
        builder = builder.enableHiveSupport()
        //.config("spark.sql.hive.metastore.version","2.3.3")
      }

      //The default number of partitions to use when shuffling data for joins or aggregations.
      if(numPartitions != 200){
        builder.config("spark.sql.shuffle.partitions",numPartitions)
      }
      val spark = builder.getOrCreate()
      //需要有jar才可以在远程执行
      //spark.sparkContext.addJar("/opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/target/spark-scala-maven-2.4.0-1.0-SNAPSHOT.jar")



      spark.sparkContext.setLogLevel("WARN")
      spark
    }

  }


  /**
    * 得到当前工程的路径
    * @return
    */
  def getProjectPath:String=System.getProperty("user.dir")


}
