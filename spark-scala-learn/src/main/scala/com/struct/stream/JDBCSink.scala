package com.struct.stream

import java.sql._

import org.apache.spark.sql.{ForeachWriter, Row}

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
