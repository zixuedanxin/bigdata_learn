package ingestion

/**
  * 每天课程访问次数的实体类
  * @param day
  * @param cmsId
  * @param times
  */

case class DayVideoAccessStat(day: String, cmsId: Long, times: Long)


