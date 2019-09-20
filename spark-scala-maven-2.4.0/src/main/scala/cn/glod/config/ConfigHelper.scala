package cn.glod.config

/**
  * @author zhangjin
  * @create 2018-12-02 10:55
  */

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.StringSerializer

object ConfigHelper {

  private lazy val config: Config = ConfigFactory.load()

  // 原始文件所在路径
  val bz2path = config.getString("dmp.bz2.path")

  // parquet文件输出路径
  val parquetPath = config.getString("dmp.parquet.path")


  // 封装Producer参数的
  val kafkaProduceProps = new Properties()
  kafkaProduceProps.setProperty("bootstrap.servers", "kk-01:9092,kk-02:9092,kk-03:9092")
  kafkaProduceProps.setProperty("key.serializer", classOf[StringSerializer].getName)
  kafkaProduceProps.setProperty("value.serializer", classOf[StringSerializer].getName)
  kafkaProduceProps.setProperty("acks", "1")

  // 数据库的相关配置
  val dbDriver = config.getString("db.default.driver")
  val dbUrl = config.getString("db.default.url")
  private[this] val dbUser = config.getString("db.default.user")
  private[this] val dbPasswd = config.getString("db.default.password")

  val dbProps = new Properties()
  dbProps.setProperty("driver", dbDriver)
  dbProps.setProperty("user", dbUser)
  dbProps.setProperty("password", dbPasswd)


}
