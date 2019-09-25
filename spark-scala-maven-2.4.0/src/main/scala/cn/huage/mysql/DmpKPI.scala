package cn.huage.mysql

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * author: sheep.Old 
  * qq: 64341393
  * Created 2018/6/26
  */
object DmpKPI {


    def generalKpi(baseRDD: RDD[(String, String, (Int, Int, Long, Long), String, String)]): Unit = {
        // 业务概况
        val generalDayResult = baseRDD.map(tp => (tp._1, tp._3)).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4))
        val generalDayHourResult = baseRDD.map(tp => ((tp._1, tp._2), (tp._3._1, tp._3._2))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

        MySQLHandler.general2MySQL(generalDayResult) // 入库 -- cmcc_general_32
        MySQLHandler.generalDayHour2MySQL(generalDayHourResult) // 入库 -- cmcc_general_hour_32
    }


    def perMinutesKpi(baseRDD: RDD[(String, String, (Int, Int, Long, Long), String, String)]): Unit = {
        // 实时计算每分钟的充值订单及金额
        val perMinutesResult = baseRDD.map(tp => ((tp._1, tp._2, tp._5), (tp._3._2, tp._3._4))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        MySQLHandler.perMinutes2MySQL(perMinutesResult)
    }

    def provinceKpi(pCode2NameBT: Broadcast[Map[String, AnyRef]], baseRDD: RDD[(String, String, (Int, Int, Long, Long), String, String)]): Unit = {
        // 全国各省充值成功订单量分布
        val dayProvinceSuccResult = baseRDD.map(tp => ((tp._1, tp._4), tp._3._2)).reduceByKey(_ + _)
        // 入库 -- cmcc_province_day_32
        MySQLHandler.provinceDay2MySQL(dayProvinceSuccResult, pCode2NameBT)
    }

}
