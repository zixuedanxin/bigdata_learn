package com.xuzh.mysql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object InsertDataToMysql1 {

  case class User(username: String, password: String, age: Int, name: String)

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = new SparkSession
    .Builder()
      .appName("InsertDataToMysql1")
      .master("local")
      .getOrCreate()

    val data = spark.sparkContext.parallelize(List(("Jack", "123456", 20, "杰克"), ("Tom", "123456", 25, "汤姆")))

    import spark.implicits._

    val df = data.map(x => User(x._1, x._2, x._3, x._4)).toDF()
    df.write.mode(SaveMode.Append).format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/test?useSSL=false&useUnicode=true&characterEncoding=utf8")
      .option("dbtable","user")
      .option("user","root")
      .option("password","root")
      .save()

  }

}
