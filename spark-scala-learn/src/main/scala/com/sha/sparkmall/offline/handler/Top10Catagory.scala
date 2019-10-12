package com.sha.sparkmall.offline.handler

import com.sha.sparkmall.common.model.UserVisitAction
import com.sha.sparkmall.common.util.JdbcUtil
import com.sha.sparkmall.offline.acc.CategoryCountAccumulator
import com.sha.sparkmall.offline.bean.CategoryCount
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.{immutable, mutable}

/**
  * @author shamin
  * @create 2019-04-10 18:22 
  */
object Top10Catagory {
  def handle(sparkSession: SparkSession, rdd:RDD[UserVisitAction], taskId: String)= {
    //注册累加器
    val accumulator = new CategoryCountAccumulator
    sparkSession.sparkContext.register(accumulator)

    //2.利用累加器进行统计操作，得到一个map结构的统计结果
    rdd.foreach(userVisitAction => {
      if(userVisitAction.click_category_id != -1L){
        accumulator.add(userVisitAction.click_category_id + "_click")
      }else if(userVisitAction.order_category_ids != null){
        val cidArray :Array[String] = userVisitAction.order_category_ids.split(",")
        for(cid <- cidArray){
          accumulator.add(cid + "_order")
        }
      }else if(userVisitAction.pay_category_ids != null){
        val cidArray :Array[String] = userVisitAction.pay_category_ids.split(",")
        for(cid <- cidArray){
          accumulator.add(cid + "_pay")
        }
      }
    })

    val categoryCountMap: mutable.HashMap[String, Long] = accumulator.value
    println(categoryCountMap.mkString("\n"))

    //按cid进行分组
    val countGroupbyCidMap: Map[String, mutable.HashMap[String, Long]] = categoryCountMap.groupBy{case (cid_action,count) => cid_action.split("_")(0)}

    val categoryCountItr: immutable.Iterable[CategoryCount] = countGroupbyCidMap.map { case (cid, countMap) =>
      CategoryCount(taskId, cid, countMap.getOrElse(cid + "_click", 0L), countMap.getOrElse(cid + "_order", 0L), countMap.getOrElse(cid + "_pay", 0L))
    }
    val sortedCategoryCountList: List[CategoryCount] = categoryCountItr.toList.sortWith { (categoryCount1, categoryCount2) =>
      if (categoryCount1.clickCount > categoryCount2.clickCount) {
        true
      } else if (categoryCount1.clickCount == categoryCount2.clickCount) {
        if (categoryCount1.orderCount > categoryCount2.orderCount) {
          true
        } else {
          false
        }
      }
      else {
        false
      }
    }
    val top10List: List[CategoryCount] = sortedCategoryCountList.take(10)

    val top10ArrayList: List[Array[Any]] = top10List.map{categoryCount=>Array(categoryCount.taskId,categoryCount.cid,categoryCount.clickCount,categoryCount.orderCount,categoryCount.payCount)}

    JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)",top10ArrayList)

    top10List
  }
}
