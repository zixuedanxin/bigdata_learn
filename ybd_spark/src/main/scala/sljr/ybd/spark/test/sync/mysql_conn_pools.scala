package sljr.ybd.spark.test.sync

import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.slf4j.LoggerFactory
object mysql_conn_pools {
  val logger = LoggerFactory.getLogger(this.getClass)
  val host=""
  val database=""// String,port:String,username:String ,password:String
  //连接池配置
  private val connectionPool: Option[BoneCP] = {
    try {
      Class.forName("com.mysql.jdbc.Driver")//org.postgresql.Driver
      val config = new BoneCPConfig()
      config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/test_bee")
      config.setUsername("postgres")
      config.setPassword("******")
      config.setLazyInit(true)
      config.setMinConnectionsPerPartition(3)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(10)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(false)
      Some(new BoneCP(config))
    } catch {
      case exception: Exception =>
        logger.warn("Create Connection Error: \n" + exception.printStackTrace())
        None
    }
  }

  // 获取数据库连接
  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(pool) => Some(pool.getConnection)
      case None => None
    }
  }

  // 释放数据库连接
  def closeConnection(connection: Connection): Unit = {
    if (!connection.isClosed) {
      connection.close()
    }
  }
}