package cn.glod.mysql

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scalikejdbc.{DB, SQL}



/** 跟mysql数据进行交互的
  * author: sheep.Old 
  * qq: 64341393
  * Created 2018/6/26
  */
object MySQLHandler {

    /**
      * 将订单总量和成功的量入库
      *
      * @param generalResult
      */
    def general2MySQL(generalResult: RDD[(String, (Int, Int, Long, Long))]): Unit = {
        generalResult.foreachPartition(iter => {
            // 存在即更新，不存在插入 20170412 100 99
            DB.localTx { implicit session =>
                iter.foreach(tp => {
                    SQL(
                        """
                          |INSERT INTO cmcc_general_32 (`day`, total, succ, costTime, money)
                          |VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY
                          |UPDATE total = total + ?, succ = succ + ?, costTime = costTime + ?, money = money + ?
                        """.stripMargin).bind(tp._1, tp._2._1, tp._2._2, tp._2._3, tp._2._4,
                        tp._2._1, tp._2._2, tp._2._3, tp._2._4
                    ).update().apply()
                })
            }
        })
    }


    def generalDayHour2MySQL(generalResult: RDD[((String, String), (Int, Int))]): Unit = {
        generalResult.foreachPartition(iter => {
            // 存在即更新，不存在插入 20170412 100 99
            DB.localTx { implicit session =>
                iter.foreach(tp => {
                    SQL(
                        """
                          |INSERT INTO cmcc_general_hour_32 (`day`, hour, total, succ)
                          |VALUES (?, ?, ?, ?) ON DUPLICATE KEY
                          |UPDATE total = total + ?, succ = succ + ?
                        """.stripMargin).bind(tp._1._1, tp._1._2, tp._2._1, tp._2._2, tp._2._1, tp._2._2
                    ).update().apply()
                })
            }
        })
    }

    /**
      * 全国各省充值成功订单入库
      *
      * @param dayProvinceSuccResult
      * @param pCode2Name
      */
    def provinceDay2MySQL(dayProvinceSuccResult: RDD[((String, String), Int)], pCode2Name: Broadcast[Map[String, AnyRef]]): Unit = {
        dayProvinceSuccResult.foreachPartition(iter => {
            DB.localTx { implicit session =>
                iter.foreach(tp => {
                    SQL(
                        """
                          |INSERT INTO cmcc_province_day_32 (`day`, pname, pcode, succ)
                          |VALUES (?, ?, ?, ?) ON DUPLICATE KEY
                          |UPDATE succ = succ + ?
                        """.stripMargin).bind(tp._1._1, pCode2Name.value.getOrElse(tp._1._2, "未知"), tp._1._2, tp._2, tp._2
                    ).update().apply()
                })
            }
        })
    }

    /**
      * 每分钟充值笔数及金额入库
      * @param perMinutesResult
      */
    def perMinutes2MySQL(perMinutesResult: RDD[((String, String, String), (Int, Long))]) = {
        perMinutesResult.foreachPartition(iter => {
            DB.localTx { implicit session =>
                iter.foreach(tp => {
                    SQL(
                        """
                          |INSERT INTO cmcc_perminutes_32 (`day`, hour, minutes, succ, money)
                          |VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY
                          |UPDATE succ = succ + ?, money = money + ?
                        """.stripMargin)
                      .bind(
                          tp._1._1,
                          tp._1._2,
                          tp._1._3,
                          tp._2._1,
                          tp._2._2,
                          tp._2._1,
                          tp._2._2
                      ).update().apply()
                })
            }
        })
    }


}
