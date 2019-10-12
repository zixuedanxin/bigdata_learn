package streaming

import java.net.InetAddress

import org.apache.spark.sql.SparkSession

object SparkInstance {
  val sparkSession = SparkSession.builder.master("local[*]").appName(InetAddress.getLocalHost.getHostName).getOrCreate()
  val sparkContext = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext
  println("Initialized Spark instance.")

  sys.addShutdownHook {
    sparkSession.stop()
    println("Terminated Spark instance.")
  }
}
