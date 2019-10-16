package org.roy.demo.streaming.utils

import java.sql._

import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * create roy by 2019/09/09
  * sink to mysql
  *
  * @param url
  * @param user
  * @param pwd
  */
//(Int, Double, Boolean, String, String, String)
class JDBCSink(url: String, user: String, pwd: String) extends ForeachWriter[Row] {
  val driver = "com.mysql.jdbc.Driver"
  var connection: Connection = _
  var statement: Statement = _

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }

  def process(value: Row): Unit = {
    //storeName,provinceId,provinceName,cityId,cityName,
    //    val sdate = value.getAs[String]("orderDate")
    val sql =
    s"""
       |INSERT INTO order_stream_sink (orderNum,orderMoney,expired,storeId,otype,orderDate)
       |VALUES (${value.getAs[Int]("orderNum")},${value.getAs[Double]("orderMoney")},
       |${value.getAs[Boolean]("expired")},${value.getAs[Int]("storeId")},
       |${value.getAs[Int]("otype")},"${value.getAs[String]("orderDate")}")
       |ON DUPLICATE KEY UPDATE  orderNum=${value.getAs[Int]("orderNum")},
       |orderMoney=${value.getAs[Double]("orderMoney")}
      """.stripMargin
    println("sql==" + sql)
    statement.executeUpdate(sql)
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }

}