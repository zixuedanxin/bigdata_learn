package com.xuzh.mysql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame

object ReadMysqlData {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark=new sql.SparkSession
    .Builder()
      .appName("ReadMysqlData")
      .master("local")
      .getOrCreate()
    val jdbc_conf: Map[String, String] = Map(
      "url" -> "jdbc:mysql://localhost:3306/sapi_n",
      "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "t_apps",
      "user" -> "root",
      "password" -> "root"
    )
    val data_mysql: DataFrame = spark.read.format("jdbc")
      .options(jdbc_conf)
      .load()

    data_mysql.show()

  }

}
