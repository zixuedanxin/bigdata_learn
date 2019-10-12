package com.sha.sparkmall.offline.app

import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import com.sha.sparkmall.common.model.UserVisitAction
import com.sha.sparkmall.common.util.{JdbcUtil, PropertiesUtil}
import com.sha.sparkmall.offline.acc.CategoryCountAccumulator
import com.sha.sparkmall.offline.bean.CategoryCount
import com.sha.sparkmall.offline.handler.{PageConverRatioHandler, Top10Catagory, Top10CategoryTop10Session}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.{immutable, mutable}

/**
  * @author shamin
  * @create 2019-04-09 18:45
  */
object OfflineApp {
  def main(args: Array[String]): Unit = {
    val taskId: String = UUID.randomUUID().toString

    val sparkConf = new SparkConf().setAppName("OfflineApp").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //获取hive的数据库名称
    val properties = PropertiesUtil.load("config.properties")
    val database = properties.getProperty("hive.database")

    //获取条件配置文件
    val conditionProperties = PropertiesUtil.load("conditions.properties")
    val conditionsJson: String = conditionProperties.getProperty("condition.params.json")
    val conditionsJsonObj: JSONObject = JSON.parseObject(conditionsJson)
    val startDate = conditionsJsonObj.getString("startDate")
    val endDate = conditionsJsonObj.getString("endDate")
    val startAge = conditionsJsonObj.getString("startAge")
    val endAge: String = conditionsJsonObj.getString("endAge")

    //过滤掉不满足条件的数据
    sparkSession.sql("use " + database)
    var sql = "select uv.* " +
      "from user_visit_action uv join user_info ui on uv.user_id = ui.user_id where";
    if (startDate != null && startDate.length > 0) {
      sql += " uv.date >= '" + startDate + "'"
    }
    if (endDate != null && endDate.length > 0) {
      sql += " and uv.date <= '" + endDate+ "'"
    }
    if (startAge != null && startAge.length > 0) {
      sql += " and ui.age >= " + startAge
    }
    if (endAge != null && endAge.length > 0) {
      sql += " and ui.age <= " + endAge
    }
    import sparkSession.implicits._
    //将DataFrame转换为rdd
    val rdd: RDD[UserVisitAction] = sparkSession.sql(sql).as[UserVisitAction].rdd

    //需求一
   // val top10CategroyCount: List[CategoryCount] = Top10Catagory.handle(sparkSession,rdd,taskId)
    println("需求一  成功")
    //需求二
   // val unit: Unit = Top10CategoryTop10Session.handle(sparkSession,rdd,top10CategroyCount,taskId)
    println("需求二  成功")

    PageConverRatioHandler.handle(sparkSession,rdd,taskId,conditionsJsonObj)
    println("需求四 成功")
}
}
