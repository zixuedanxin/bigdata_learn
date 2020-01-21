package com.xuzh.mysql

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object InsertDataToMysql0 {
//https://www.cnblogs.com/lillcol/p/9796935.html
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
/*
object sparkToMysql {

	case class info(info1: String, info2: Int)

	def toMySQL(iterator: Iterator[(String, Int)]): Unit = {
		var conn: Connection = null
		var ps: PreparedStatement = null
		val sql = "insert into info(info1, info2) values (?, ?)"
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_test", "root", "111111")
			iterator.foreach(dataIn => {
				ps = conn.prepareStatement(sql)
				ps.setString(1, dataIn._1)
				ps.setInt(2, dataIn._2)
				ps.executeUpdate()
			}
			)
		} catch {
			case e: Exception => e.printStackTrace()
		} finally {
			if (ps != null) {
				ps.close()
		}
			if (conn != null) {
				conn.close()
			}
		}
	}

	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("sparkToMysql").setMaster("local")
		val sc = new SparkContext(conf)
		val dataFromHDFS=sc.textFile(args(0)).map(_.split("\\^")).map(line => (line(0),line(1).toInt))
		dataFromHDFS.foreachPartition(toMySQL)
	}
}
 */