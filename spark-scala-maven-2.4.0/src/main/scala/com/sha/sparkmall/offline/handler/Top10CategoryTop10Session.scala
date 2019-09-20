package com.sha.sparkmall.offline.handler

import com.sha.sparkmall.common.model.UserVisitAction
import com.sha.sparkmall.common.util.JdbcUtil
import com.sha.sparkmall.offline.bean.CategoryCount
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author shamin
  * @create 2019-04-10 10:45 
  */
object Top10CategoryTop10Session {
    //需求：求top10热门品类中Top10活跃Session
  //cid seesionId

  //1.过滤 过滤到不是top10热门品类的数据
  def handle(sparkSession: SparkSession,rdd: RDD[UserVisitAction],top10CategroyCount: List[CategoryCount],taskId:String){
    //1.过滤
    //将top10CategoryCount的数据广播到每个executor中
    val top10CategoryId: List[String] = top10CategroyCount.map(_.cid)
    val top10CategoryCountBC: Broadcast[List[String]] = sparkSession.sparkContext.broadcast(top10CategoryId)
    val CategoryDataFilter: RDD[UserVisitAction] = rdd.filter({ userVisitAction =>
      //这里过滤的时候一定要注意类型
      top10CategoryCountBC.value.contains(userVisitAction.click_category_id.toString)
    })
    //2.将数据调整格式为：((cid,sessionId),1)
    val cidSessionByOne: RDD[((Long, String), Int)] = CategoryDataFilter.map({
      userVisitAction =>
        ((userVisitAction.click_category_id, userVisitAction.session_id), 1)
    })
    //3.按key(cid,sessionId)进行统计点击次数
    val cidSessionByCount: RDD[((Long, String), Int)] = cidSessionByOne.reduceByKey(_+_)

    //4.将数据格式调整为(cid,CategorySession),变换最小粒度,并按cid进行分组
    val cidSessionCount: RDD[(Long, Iterable[CategorySession])] = cidSessionByCount.map({ case ((cid, sessionId), count) =>
      (cid, CategorySession(cid,sessionId, count,taskId))
    }).groupByKey()
    //5.然后再每个cid组里排序并取前10
    val top10CategorySession: RDD[(Long,Iterable[CategorySession])] = cidSessionCount.map({ case (cid, categorySession) =>
      //将集合转换为List之后才可以使用sortWith比较
      val top10CategorySession: List[CategorySession] = categorySession.toList.sortWith(_.count > _.count).take(10)
      (cid,top10CategorySession)
    })
    //6.将(cid,Iterable()) 压平写入Array
    val top10CateSessionArry: RDD[Array[Any]] = top10CategorySession.flatMap({ case (cid, top10CategorySessionIter) =>
      val top10CategorySessionArray: Iterable[Array[Any]] = top10CategorySessionIter.map({ case (categorySession) =>
        Array(categorySession.taskId,categorySession.cid, categorySession.sessionId, categorySession.count)
      })
      top10CategorySessionArray
    })
    val top10CateSessionlist: List[Array[Any]] = top10CateSessionArry.collect().toList

    JdbcUtil.executeBatchUpdate("insert into top10_category_session values(?,?,?,?)",top10CateSessionlist)
  }
  case class CategorySession(cid:Long,sessionId:String,count:Long,taskId:String)
}
