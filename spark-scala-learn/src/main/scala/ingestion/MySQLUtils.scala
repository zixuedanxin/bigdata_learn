package ingestion

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * MySql操作工具类
  */
object MySQLUtils {

  /**
    * 获取连接数据库
    */

   def getConnection() = {

     DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_project?user=root&password=shujuelin321")

   }


  /**
    * 释放数据库连接等资源
    * @param connection
    * @param pstmt
    */
   def release(connection: Connection, pstmt: PreparedStatement): Unit = {

    try{

      pstmt.close()

    }catch {

      case e:Exception => e.printStackTrace()
    }finally {

      if (connection != null){

        pstmt.close()
      }
    }


   }

  def main(args: Array[String]): Unit = {

    println(getConnection())

  }
}
