package imook




import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat



/**
  * 日期时间解析工具类
  */
object DataUtlis {

  //先定义一个原来的格式[10/Nov/2016:00:01:02 +0800] 相当于定义了一个输入文件日期的格式
  val YYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  //目标日期格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    * 获取时间yyyy-MM-dd HH:mm:ss
    * @param time
    * @return
    */
  def parse(time: String) = {

    TARGET_FORMAT.format(new Date(getTime(time)))

  }

  /**
    * 获取输入日志时间:long类型
    * @param time[10/Nov/2016:00:01:02 +0800]
    * @return
    */
  def getTime(time: String) = {

     try{

       YYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[")+1, time.lastIndexOf("]"))).getTime
     }catch {

       case e: Exception => {
         0L
       }
     }

  }
}
