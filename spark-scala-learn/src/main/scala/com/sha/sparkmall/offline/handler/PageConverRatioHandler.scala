package com.sha.sparkmall.offline.handler

import com.alibaba.fastjson.JSONObject
import com.sha.sparkmall.common.model.UserVisitAction
import com.sha.sparkmall.common.util.JdbcUtil
import com.sha.sparkmall.offline.bean.PageConvertRatio
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author shamin
  * @create 2019-04-12 9:27 
  */
object PageConverRatioHandler {
  def handle(sparkSession: SparkSession, userVisitAction: RDD[UserVisitAction], taskID: String, conditionsJsonObj: JSONObject): Unit = {
    val targetPageFlow: String = conditionsJsonObj.getString("targetPageFlow")
    val targetPageFlowArray: Array[String] = targetPageFlow.split(",")
    val fromtargetPageFlow: Array[String] = targetPageFlowArray.slice(0, targetPageFlowArray.length - 1)
    //1.求单个页面的访问量
    //将目标数组广播
    val targetPageFlowBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(fromtargetPageFlow)
    //1.1过滤掉非目标的page_id
    val targetPageActionRDD: RDD[UserVisitAction] = userVisitAction.filter(userVisitAction =>
      targetPageFlowBC.value.contains(userVisitAction.page_id.toString)
    )
    //1.1单个页面的访问量
    val pageVisitTotal: collection.Map[Long, Long] = targetPageActionRDD.map(userVisitAction => (userVisitAction.page_id, 1L)).countByKey()

    //在页面跳转中的目标页面跳转
    val toTargetPageFlow: Array[String] = targetPageFlowArray.slice(1, targetPageFlowArray.length)
    val fromPageToPageArray: Array[String] = fromtargetPageFlow.zip(toTargetPageFlow).map { case (fromPage, toPage) => fromPage + "-" + toPage }
    val fromPageToPageBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(fromPageToPageArray)
    //2.求页面之间的跳转次数
    //结果：(1-2,100),(2-3,50),(3-4,30)......
    //思路，跳转是在同一个session之间的跳转才有意义
    //2.1首先按sessionId进行分组(sessionId,userVisitAction)
    val sessionIdGroupRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitAction.map { case userVisitAction =>
      (userVisitAction.session_id, userVisitAction)
    }.groupByKey
    //2.2在将sessionId内的分组按时间降序排序，得到sessionId内的pageId的排序(sessionId,list(1,3,2,4))
    val pageJumpRDD: RDD[String] = sessionIdGroupRDD.flatMap { case (sessionId, userVisitAction) =>
      val actionsSortedByTime: List[UserVisitAction] = userVisitAction.toList.sortWith(_.action_time < _.action_time)
      //得到sessionId内的pageId的排序(sessionId,list(1,3,2,4))
      val pageIdList: List[Long] = actionsSortedByTime.map(_.page_id)
      //2.3调整结构为((1,3),(4,11),(3,4),......))
      val fromPageList: List[Long] = pageIdList.slice(0, pageIdList.length - 1)
      val toPageList: List[Long] = pageIdList.slice(1, pageIdList.length)
      val pageJumpList: List[(Long, Long)] = fromPageList.zip(toPageList)
      //2.4将数据调整为(1-3,4-11,3-4......)
      val fromToPageList: List[String] = pageJumpList.map { case (fromPageId, toPageId) =>
        fromPageId + "-" + toPageId
      }
      //这里要过滤掉不是(1-2,2-3,3-4...)的数据 ,过滤的话就要想到将目标数组声明为广播变量
      val fromToPage: List[String] = fromToPageList.filter { pageJump =>
        fromPageToPageBC.value.contains(pageJump)
      }
      //因为这里已经不需要sessionId,所以可以将得到的结果压平
      //得到的结果就是(1-2,1-2,2-3...)的数据
      fromToPage
    }
    //2.5计算跳转页面的次数((1-2,100),(2-3,50),(3-4,20))
    val pageJumpCount: collection.Map[String, Long] = pageJumpRDD.map((_, 1)).countByKey()

    //2.5然后计算页面单跳比率(1-2,100)/(1,200)
    val pageJumpRatio: Iterable[Array[Any]] = pageJumpCount.map { case (pageJump, count) =>
      val pageId: String = pageJump.split("-")(0)
      val totalCount: Long = pageVisitTotal.getOrElse(pageId.toLong, -1L)
      val pageConvertRatio: Double = Math.round(count.toDouble * 1000 / totalCount) / 10D
      Array(taskID, pageJump, pageConvertRatio)
    }

    //2.6将结果保存到Mysql中
    JdbcUtil.executeBatchUpdate("insert into page_convert_ratio values(?,?,?)",pageJumpRatio)

  }
}
