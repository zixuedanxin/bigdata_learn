package com.xuzh.mysql

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object InsertDataToMysql0 {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = new SparkSession
    .Builder()
      .appName("InsertDataToMysql0")
      .master("local")
      .getOrCreate()

    val data = spark.sparkContext.parallelize(List(("Jack", "123456", 20, "杰克"), ("Tom", "123456", 25, "汤姆")))

    var conn: Connection = null
    var ps: PreparedStatement = null

    val sql = "insert into user(username,password,age,name) values(?,?,?,?)"

    data.foreachPartition(
      rdd =>
        try{
          conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8", "root", "root")
          rdd.foreach(data => {
            ps = conn.prepareStatement(sql)
            ps.setString(1,data._1)
            ps.setString(2,data._2)
            ps.setInt(3,data._3)
            ps.setString(4,data._4)
            ps.executeUpdate()
          })

        }catch {
          case e: Exception => println("Mysql Exception"+e.getMessage)
        }finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close()//关闭mysql连接
          }
        }


    )


  }

}
