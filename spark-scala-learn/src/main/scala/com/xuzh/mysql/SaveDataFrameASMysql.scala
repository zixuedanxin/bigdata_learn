
import java.io.FileInputStream
import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Administrator
  *         2018/10/16-10:15
  *https://www.cnblogs.com/lillcol/p/9796935.html
  */
object SaveDataFrameASMysql {
  var hdfsPath: String = ""
  var proPath: String = ""
  var DATE: String = ""

  val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName)
  val sc: SparkContext = new SparkContext(sparkConf)
  val sqlContext: SQLContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    hdfsPath = args(0)
    proPath = args(1)
    //不过滤读取
    val dim_sys_city_dict: DataFrame = readMysqlTable(sqlContext, "TestMysqlTble1", proPath)
    dim_sys_city_dict.show(10)

    //保存mysql
    saveASMysqlTable(dim_sys_city_dict, "TestMysqlTble2", SaveMode.Append, proPath)
  }

  /**
    * 将DataFrame保存为Mysql表
    *
    * @param dataFrame 需要保存的dataFrame
    * @param tableName 保存的mysql 表名
    * @param saveMode  保存的模式 ：Append、Overwrite、ErrorIfExists、Ignore
    * @param proPath   配置文件的路径
    */
  def saveASMysqlTable(dataFrame: DataFrame, tableName: String, saveMode: SaveMode, proPath: String) = {
    var table = tableName
    val properties: Properties = getProPerties(proPath)
    val prop = new Properties //配置文件中的key 与 spark 中的 key 不同 所以 创建prop 按照spark 的格式 进行配置数据库
    prop.setProperty("user", properties.getProperty("mysql.username"))
    prop.setProperty("password", properties.getProperty("mysql.password"))
    prop.setProperty("driver", properties.getProperty("mysql.driver"))
    prop.setProperty("url", properties.getProperty("mysql.url"))
    if (saveMode == SaveMode.Overwrite) {
      var conn: Connection = null
      try {
        conn = DriverManager.getConnection(
          prop.getProperty("url"),
          prop.getProperty("user"),
          prop.getProperty("password")
        )
        val stmt = conn.createStatement
        table = table.toUpperCase
        stmt.execute(s"truncate table $table") //为了不删除表结构，先truncate 再Append
        conn.close()
      }
      catch {
        case e: Exception =>
          println("MySQL Error:")
          e.printStackTrace()
      }
    }
    dataFrame.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"), table, prop)
  }

  /**
    * 获取 Mysql 表的数据
    *
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @param proPath   配置文件的路径
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String, proPath: String) = {
    val properties: Properties = getProPerties(proPath)
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql.password"))
      //        .option("dbtable", tableName.toUpperCase)
      .option("dbtable", tableName)
      .load()

  }

  /**
    * 获取 Mysql 表的数据 添加过滤条件
    *
    * @param sqlContext
    * @param table           读取Mysql表的名字
    * @param filterCondition 过滤条件
    * @param proPath         配置文件的路径
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, table: String, filterCondition: String, proPath: String) = {
    val properties: Properties = getProPerties(proPath)
    var tableName = ""
    tableName = "(select * from " + table + " where " + filterCondition + " ) as t1"
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql.password"))
      .option("dbtable", tableName)
      .load()
  }

  /**
    * 获取配置文件
    *
    * @param proPath
    * @return
    */
  def getProPerties(proPath: String) = {
    val properties: Properties = new Properties()
    properties.load(new FileInputStream(proPath))
    properties
  }
}



