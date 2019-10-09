package com.xuzh.log

//按照地市进行统计TopN课程  实体类
case class DayCityVideoAccessStat(day:String, cmsId:Long, city:String,times:Long,timesRank:Int)
