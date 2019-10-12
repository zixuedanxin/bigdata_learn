package sljr.ybd.spark.test

import java.sql.{Connection, DriverManager}

object ScalaJDBC {
  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.188.80/yfgtest"
    val username = "test"
    val password = "123456"
    var connection:Connection = null
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement()
      val sql="select concat(TABLE_SCHEMA,':',TABLE_NAME) tb,GROUP_CONCAT(COLUMN_NAME) key_cols" +
        " from information_schema.key_column_usage where CONSTRAINT_NAME='PRIMARY' and TABLE_SCHEMA<>'mysql' " +
        " GROUP BY TABLE_SCHEMA ,TABLE_NAME"
      val resultSet = statement.executeQuery(sql)
      while ( resultSet.next() ) {
        val name = resultSet.getString("tb")
        val password = resultSet.getString("key_cols")
        println("name, password = " + name + ", " + password)
      }
    } catch {
      case e => e.printStackTrace
      //case _: Throwable => println("ERROR")
    }
    connection.close()
  }

}