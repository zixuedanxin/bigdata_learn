package ingestion

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


/**
  * 访问日志转换(输入转==> 输出)工具类
  */
object AccessConvertUtils {

  //用结构体定义输出的字段
    val struct = StructType(

    Array(
      StructField("url", StringType), //访问的url
      StructField("cmsType", StringType),//课程类型
      StructField("cmsId", LongType),//编号
      StructField("traffic",LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)//分区字段
    )
  )

  /**
    * 根据输入的每一行信息转换成输出的样式
    * @param log  输入的每一行记录信息
    */
   def parsLog(log: String) = {

     try{

       val splits = log.split("\t")

       val url = splits(1)

       val traffic = splits(2).toLong //因为我们的流量是string类型，而我们定义的输出类型是Long.所以要转换

       val ip = splits(3)

       val domain = "http://www.imooc.com/"
       val cms = url.substring(url.indexOf(domain) + domain.length)
       val cmsTypeId = cms.split("/")

       var cmsType = ""
       var cmsId = 0l
       if(cmsTypeId.length > 0 ){

         cmsType = cmsTypeId(0)
         cmsId = cmsTypeId(1).toLong
       }

       val city = IpUtils.getCity(ip)
       val time = splits(0)
       val day = time.substring(0, 10).replaceAll("-", "")

       //这个row里面的字段要和struct中的字段对应起来
       Row(url, cmsType, cmsId, traffic, ip, city, time, day)
     } catch {

       case e:Exception => Row(0)

     }

   }
}
