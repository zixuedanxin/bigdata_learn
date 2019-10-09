package com.xuzh.utils

object ParamsUtils {

  // kafka配置参数
  object kafka {
    val KAFKA_PARAMS = Map[String, String](
      "metadata.broker.list" -> ConfigUtils.KAFKA_SERVERS,
      "group.id" -> ConfigUtils.kAFKA_CONSUMER_GROUPID,
      "auto.commit.enable" -> "false",
      "auto.offset.reset" -> "largest")
    val KAFKA_TOPIC = ConfigUtils.KAFKA_TOPIC.split(",").toSet
  }

  // mysql配置参数
  object mysql{
    val mysqlProp = new java.util.Properties
    mysqlProp.setProperty("user", "root")
    mysqlProp.setProperty("password", "root")
    mysqlProp.setProperty("driver", "com.mysql.jdbc.Driver")  //"jdbc:mysql://localhost:3306"
  }


}
