package sljr.ybd.spark.test

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import jodd.util.PropertiesUtil
import org.codehaus.jettison.json._
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata

import scala.collection.JavaConversions._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.streaming.dstream.InputDStream


object scala_test {
  //mysql配置信息
  val url = "jdbc:mysql://192.168.188.80/yfgtest"
  val user = "test"
  val password = "123456"
  val offsettable = "yfgtest.streaming_task2"
  val diu_type_in_hbase = Map("delete" -> 0, "insert" -> 1, "update" -> 2)

  var need2synctables = Map[String,String]()
  //need2synctables = get_mysql_metadata(url, user, password)
  //  var no2synctables = collection.mutable.Buffer[String]()
  var hbase_exist_tables = collection.mutable.Buffer[String]()
  //

  //分割字符
  val splitstr1 = "YBD##1##YBD"
  val splitstr2 = "YBD##2##YBD"
  //kafka配置信息
  val topics = Set("maxwell")
  val group_id = "test2hbase"
  val brokers = "cdh2:9092,cdh3:9092"
  //hbsae配置信息
  val zkips = "192.168.188.80,192.168.188.81,192.168.188.82"
  val hconf = HBaseConfiguration.create()
  hconf.set("hbase.zookeeper.quorum", zkips)
  hconf.set("hbase.zookeeper.property.clientPort", "2181")
  val jobConf = new JobConf(hconf, this.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  //获取hbase链接
  def get_hbsae_connection(): org.apache.hadoop.hbase.client.Connection = {
    //设置zookeeper地址及端口
    //val zkips = "192.168.188.80,192.168.188.81,192.168.188.82"
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", zkips)
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    val hconnection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hconf)
    return hconnection
  }


  def get_hbase_namespaces(): collection.mutable.Buffer[String] = {

    val namespace_list = collection.mutable.Buffer[String]()
    val hconnection = get_hbsae_connection()
    val admin = hconnection.getAdmin()
    val namespacelist = admin.listNamespaceDescriptors()
    for (i <- namespacelist) {
      namespace_list += i.getName()
    }
    admin.close()
    return namespace_list
  }


  def get_all_hbsae_tables(): collection.mutable.Buffer[String] = {
    val htables_list = collection.mutable.Buffer[String]()
    val hconnection = get_hbsae_connection()
    val admin = hconnection.getAdmin()
    val htables = admin.listTables()
    for (i <- htables) {
      htables_list += i.getNameAsString
    }
    admin.close()
    return htables_list
  }

  def main(args: Array[String]): Unit = {
    //获取上次消费的offset
    var start_offset: String = ""
    val zkips = "192.168.188.80,192.168.188.81,192.168.188.82"
    hbase_exist_tables = get_all_hbsae_tables()
    print(hbase_exist_tables)
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val a = sc.parallelize(1 to 9, 3)
    def mapDoubleFunc(a : Int) : (Int,Int) = {
      (a,a*2)
    }
    val mapResult = a.map(mapDoubleFunc)

    println(mapResult.collect().mkString)

  }
}


