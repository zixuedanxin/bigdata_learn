package sparkml.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

object CollaborativeFilteringSpark {
  //屏蔽不必要的日志显示在终端上
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

  val conf= new SparkConf().setMaster("local[1]").setAppName("CollaborativeFilteringSpark")
  val sc = new SparkContext(conf)
  val users = sc.parallelize(Array("aaa","bbb","ccc","ddd", "eee"))//设置用户名
  val films = sc.parallelize(Array("StarWar","Spider","Ghost","Beauty","Hero"))//设置电影名
  val source = Map[String,Map[String,Int]]()//用来存储user对每个电影的打分
  val filmsource = Map[String,Int]()
  def getSource() : Map[String,Map[String,Int]] = {//设置电影评分
    val user1FilmSource = Map("StarWar" -> 2,"Spider"->3,"Ghost"->1,"Beauty" -> 0,"Hero" -> 1)
    val user2FilmSource = Map("StarWar" -> 1,"Spider"->2,"Ghost"->2,"Beauty" -> 1,"Hero" -> 4)
    val user3FilmSource = Map("StarWar" -> 2,"Spider"->1,"Ghost"->0,"Beauty" -> 1,"Hero" -> 4)
    val user4FilmSource = Map("StarWar" -> 3,"Spider"->2,"Ghost"->0,"Beauty" -> 5,"Hero" -> 3)
    val user5FilmSource = Map("StarWar" -> 5,"Spider"->3,"Ghost"->1,"Beauty" -> 0,"Hero" -> 2)
    source += ("aaa" -> user1FilmSource)
    source += ("bbb" -> user2FilmSource)
    source += ("ccc" -> user3FilmSource)
    source += ("ddd" -> user4FilmSource)
    source += ("eee" -> user5FilmSource)
    source
  }
  //两两计算分值，采用余弦相似性
  def getCollaborateSource(user1: String, user2: String): Double = {
    //获得1，2两个用户的评分
    val user1FilmSource = source.get(user1).get.values.toVector
    val user2FilmSource = source.get(user2).get.values.toVector

    //对公式部分分子进行计算
    val member = user1FilmSource.zip(user2FilmSource).map(d => d._1 * d._2).reduce(_ + _).toDouble
    //求出分母第一个变量值
    val temp1 = math.sqrt(user1FilmSource.map(num => {
      math.pow(num, 2)
    }).reduce(_ + _))
    //求出分母第二个变量值
    val temp2 = math.sqrt(user2FilmSource.map(num => {
      math.pow(num, 2)
    }).reduce(_ + _))
    //求出分母
    val denominator = temp1 * temp2
    //进行计算
    member / denominator
  }

  def main(args: Array[String]): Unit = {
    getSource()
    val name = "aaa"
    users.foreach(user => {
      println(name + " 相对于 " + user + "的相似性分数为:" + getCollaborateSource(name, user))
    })
  }
}
