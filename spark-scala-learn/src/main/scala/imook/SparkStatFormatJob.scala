package imook

import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗：抽取出我们所需要的指定列的数据
  */
object SparkStatFormatJob {

  def main(args: Array[String]): Unit = {

    //拿到spark
    val spark = SparkSession.builder.appName("SparkStatFormatJob")
              .master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("file:///E:/myData/大数据开发项目/慕课日志spark/data/access.20161111.log") //拿到的是RDD类型数据
    //原数据的一条183.162.52.7 - - [10/Nov/2016:00:01:02 +0800] "POST /api3/getadv HTTP/1.1" 200 813 "www.imooc.com" "-" cid=0&timestamp=1478707261865&uid=2871142&marking=androidbanner&secrect=a6e8e14701ffe9f6063934780d9e2e6d&token=f51e97d1cb1a9caac669ea8acc162b96 "mukewang/5.0.0 (Android 5.1.1; Xiaomi Redmi 3 Build/LMY47V),Network 2G/3G" "-" 10.100.134.244:80 200 0.027 0.027
        //access.take(10).foreach(println)

    access.map(line => {

      val splits = line.split(" ") // 按照分隔符把每个字段解析出来

      val ip = splits(0)  //ip地址
      /**
        * 原始日志的第三个和第四个字段拼接起来就是完整的访问时间
        * [10/Nov/2016:00:01:02 +0800] =》 yyyy-mm-dd hh:mm:ss*/
      //(ip,DataUtlis.parse(time),url,traffic)
      val time = splits(3)+" "+splits(4)  //时间
      //"http://www.imooc.com/code/1852"多了个引号，要把引号替换
      val url = splits(11).replaceAll("\""," ")//url，“”把引号替换，因为url是被引号包起来的
      val traffic = splits(9)  //流量

      DataUtlis.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip //每个字段之间采用Tab键作分割
    }).take(10).foreach(println) //saveAsTextFile("file:///E:/myData/大数据开发项目/慕课日志spark/data/output/")

    spark.stop()
  }
}
