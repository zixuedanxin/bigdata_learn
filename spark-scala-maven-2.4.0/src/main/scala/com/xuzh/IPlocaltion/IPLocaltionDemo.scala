package com.xuzh.IPlocaltion

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 在互联网中，我们经常会见到城市热点图这样的报表数据，例如在百度统计中，
  * 会统计今年的热门旅游城市、热门报考学校等，会将这样的信息显示在热点图中。
  * 因此，我们需要通过日志信息（运行商或者网站自己生成）和城市ip段信息来判
  * 断用户的ip段，统计热点经纬度。
  * 数据格式:
  * 基站数据格式ip.txt:
  * 开始ip|结束ip|开始数字|结束数字|洲|国家|省|市|区|运营商|行政区域|英文|代码|经度|纬度
  * 1.0.0.1|1.0.3.255|16777472|16778329|亚洲|中国|福建|福州|电信|350100|China|CN|119.306249|26.075302
  * 目标数据格式20090121000132.394251.http.format:
  * 2019071912312313|125.213.100.123|www.baidu.com|/shoplist.php?phpfile=shoplist&style=1&sex=137|Mozilla/4.0
  *
  */
object IPLocaltionDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IPLocaltionDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //todo:读取基站数据
    val data: RDD[String] = sc.textFile("d:\\data\\ip.txt")

    //todo:对基站数据进行切分 ，获取需要的字段 （ipStart,ipEnd,城市位置，经度，纬度）
    val jizhanRdd: RDD[(String, String, String, String, String)] = data.map(_.split("\\|")).map(x =>
      (x(2), x(3), x(4) + "-" + x(5) + "-" + x(6) + "-" + x(7) + "-" + x(8), x(13), x(14))
    )

    //todo:获取RDD的数据
    val jizhanData: Array[(String, String, String, String, String)] = jizhanRdd.collect()

    //todo:广播变量，一个只读的数据区，所有的task都能读到的地方
    val jizhanBroadcast: Broadcast[Array[(String, String, String, String, String)]] = sc.broadcast(jizhanData)

    //todo:读取目标数据
    val destData: RDD[String] = sc.textFile("d:\\data\\20090121000132.394251.http.format")

    //todo:获取数据中的ip地址字段
    val ip = destData.map(_.split("\\|")).map(x => x(1))

    //todo:把IP地址转化为long类型，然后通过二分法去基站数据中查找，找到的维度做wordCount
    val result: RDD[((String, String), Int)] = ip.mapPartitions(iter => {
      //todo:获取广播变量中的值
      val valueArr: Array[(String, String, String, String, String)] = jizhanBroadcast.value

      //todo:操作分区中的itertator
      iter.map(ip => {
        //todo:将ip转换成long类型
        val ipNum: Long = ipToLong(ip)

        //拿这个数字long去基站数据中通过二分法查找，返回ip在valueArr中的下标
        val index: Int = binarySearch(ipNum, valueArr)

        //根据下标获取对一个的经纬度
        val tuple = valueArr(index)

        //返回结果 ((经度，维度)，1)
        ((tuple._4, tuple._5), 1)

      })

    })

    //todo:分组聚合
    val resultFinal: RDD[((String, String), Int)] = result.reduceByKey(_ + _)

    //todo:打印输出
    resultFinal.foreach(println)

    //todo:将结果保存到mysql表中
    resultFinal.map(x => (x._1._1, x._1._2, x._2)).foreachPartition(dataToMysql)

    sc.stop()

  }

  def ipToLong(ip: String): Long = {
    //todo:切分ip地址
    val ipArr: Array[String] = ip.split("\\.")
    var ipNum = 0L
    for (i <- ipArr) {
      ipNum = i.toLong | ipNum << 8L
    }

    ipNum

  }

  //todo:通过二分查找法,获取ip在广播变量中的下标
  def binarySearch(ipNum: Long, valueArr: Array[(String, String, String, String, String)]): Int = {
    //todo:口诀：上下循环寻上下，左移右移寻中间
    //开始下标
    var start = 0

    //结束下标
    var end = valueArr.length - 1

    while (start <= end) {
      val middle = (start + end) / 2

      if (ipNum >= valueArr(middle)._1.toLong && ipNum <= valueArr(middle)._2.toLong) {
        return middle
      }
      if (ipNum > valueArr(middle)._2.toLong) {
        start = middle
      }

      if (ipNum < valueArr(middle)._1.toLong) {
        end = middle
      }
    }
    -1
  }

  //todo:数据保存到mysql表中
  def dataToMysql(iterator: Iterator[(String, String, Int)]): Unit = {
    //todo:创建数据库连接Connection
    var conn: Connection = null

    //todo:创建PreparedStatement对象
    var ps: PreparedStatement = null

    //todo:采用拼占位符问号的方式写sql语句。
    var sql = "insert into iplocaltion(longitude,latitude,total_count) values(?,?,?)"

    //todo:获取数据连接
    conn = DriverManager.getConnection("jdbc:mysql://itcast01:3306/spark", "root", "root")

    //todo:  选中想被try/catch包围的语句 ctrl+alt+t 快捷键选中try/catch/finally
    try {
      iterator.foreach(line => {
        //todo:预编译sql语句
        ps = conn.prepareStatement(sql)

        //todo:对占位符设置值，占位符顺序从1开始，第一个参数是占位符的位置，第二个参数是占位符的值。
        ps.setString(1, line._1)
        ps.setString(2, line._2)
        ps.setLong(3, line._3)

        //todo:执行
        ps.execute()
      })
    } catch {
      case e: Exception => println(e)
    } finally {
      if (ps != null) {
        ps.close()
      }

      if (conn != null) {
        conn.close()
      }
    }
  }

}
