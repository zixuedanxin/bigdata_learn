package sljr.ybd.spark.sync

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Get, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka._
import org.apache.zookeeper.ZooKeeper
import org.codehaus.jettison.json._

import scala.collection.JavaConversions._


/**
  * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  * 　　　┏┓　　　┏┓
  * 　　┏┛┻━━━┛┻┓
  * 　　┃　　　　　　　┃
  * 　　┃　　　━　　　┃
  * 　　┃　┳┛　┗┳　┃
  * 　　┃　　　　　　　┃
  * 　　┃　　　┻　　　┃
  * 　　┃　　　　　　　┃
  * 　　┗━┓　　　┏━┛
  * 　　　　┃　　　┃神兽保佑, 永无BUG!
  * 　　　　┃　　　┃Code is far away from bug with the animal protecting
  * 　　　　┃　　　┗━━━┓
  * 　　　　┃　　　　　　　┣┓
  * 　　　　┃　　　　　　　┏┛
  * 　　　　┗┓┓┏━┳┓┏┛
  * 　　　　　┃┫┫　┃┫┫
  * 　　　　　┗┻┛　┗┻┛
  * ━━━━━━感觉又强力了一点，哟呵呵  =￣ω￣=━━━━━━
  * Description:
  * Creator:╰⊱⋛⋋FuGuang.Yu⋌⋚⊰╯
  * CreateTime: 2018/1/5.
  * Remark:
  * Usage:
  * Example:
  * ✻
  */


object sync_mysql2hbase {
  //mysql配置信息
  val url = "jdbc:mysql://192.168.188.80/yfgtest"
  val user = "test"
  val password = "123456"
  val diu_type_in_hbase = Map("delete" -> 0, "insert" -> 1, "update" -> 2)
  var need2synctables = Map[String,String]()
  need2synctables = get_mysql_metadata(url, user, password)
  var no_sync2hbase_tables = collection.mutable.Buffer[String]()
  //hbsae配置信息
  val leizuname = "data" //配置hbsae表中的列簇名称，目前仅支持一个列簇
  val zkips = "192.168.188.80,192.168.188.81,192.168.188.82"
  val hconf = HBaseConfiguration.create()
  hconf.set("hbase.zookeeper.quorum", zkips)
  hconf.set("hbase.zookeeper.property.clientPort", "2181")
  val jobConf = new JobConf(hconf, this.getClass)
  var hbase_exist_tables = collection.mutable.Buffer[String]()
  hbase_exist_tables = get_all_hbsae_tables(zkips)

  //分割字符
  val splitstr1 = "YBD##1##YBD"
  val splitstr2 = "YBD##2##YBD"
  //kafka配置信息
  val topics = Set("maxwell2partitions")
  val group_id = "test2hbase"
  val brokers = "192.168.188.81:9092,192.168.188.82:9092"
  val offsettable = "yfgtest:kafkaoffset" //kafkaoffset消费信息存储的hbase 表名
//设置日志级别
  Logger.getLogger("org").setLevel(Level.ERROR)

  //获取hbase链接
  def get_hbsae_connection(zkips:String): org.apache.hadoop.hbase.client.Connection = {
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", zkips)
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    val hconnection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hconf)
    return hconnection
  }

//获取hbase所有namespace
  def get_hbase_namespaces(): collection.mutable.Buffer[String] = {
    val namespace_list = collection.mutable.Buffer[String]()
    val hconnection = get_hbsae_connection(zkips)
    val admin = hconnection.getAdmin()
    val namespacelist = admin.listNamespaceDescriptors()
    for (i <- namespacelist) {
      namespace_list += i.getName()
    }
    admin.close()
    return namespace_list
  }

//获取hbsae所有表
  def get_all_hbsae_tables(zkips:String): collection.mutable.Buffer[String] = {
    val htables_list = collection.mutable.Buffer[String]()
    val hconnection = get_hbsae_connection(zkips)
    val admin = hconnection.getAdmin()
    val htables = admin.listTables()
    for (i <- htables) {
      htables_list += i.getNameAsString
    }
    admin.close()
    return htables_list
  }

  //hbase建立namespace及table
  def make_hbase_namespace_table(htablename: String) = {
    val hconnection = get_hbsae_connection(zkips)
    val admin = hconnection.getAdmin()
    val namespace = htablename.split(":")(0).toString
    val tablename = htablename.split(":")(1).toString
    val namespace_list = get_hbase_namespaces()
    val tablename_list = get_all_hbsae_tables(zkips)
    //命名空间不存在，则创建命名空间
    if (!namespace_list.contains(namespace)) {
      admin.createNamespace(NamespaceDescriptor.create(namespace).build())
    }
    //准备建表，设置表名
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(htablename))
    //设置列簇名
    val hColumnDescriptor = new HColumnDescriptor(leizuname)
    //将列簇设置加入HTableDescriptor
    tableDescriptor.addFamily(hColumnDescriptor)
    //建表
    admin.createTable(tableDescriptor)
    admin.close()
  }

  //获取要同步的表及对应的主键
  def get_mysql_metadata(url: String, user: String, password: String): Map[String, String] = {
    var tb_key_cols: Map[String, String] = Map()
    val driver = "com.mysql.jdbc.Driver"
    val sQLStatement = "select concat(TABLE_SCHEMA,':',TABLE_NAME) tb,GROUP_CONCAT(COLUMN_NAME) key_cols from information_schema.key_column_usage where CONSTRAINT_NAME='PRIMARY' and TABLE_SCHEMA <> 'mysql' GROUP BY TABLE_SCHEMA ,TABLE_NAME"
    var connection: java.sql.Connection = null
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, user, password)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(sQLStatement)

      while (resultSet.next()) {
        tb_key_cols += (resultSet.getString("tb") -> resultSet.getString("key_cols"))
      }
      connection.close()
      return tb_key_cols
    } catch {
      case e => e.printStackTrace
    }
    connection.close()
    return Map()
  }

  def process_json(line: String, k: String): String = {
    val str2json = new JSONObject(line)
    val result = str2json.getString(k)
    return result
  }

  def convertToHbasePut(line: String, splitstr1: String): (ImmutableBytesWritable, Put) = {
    val line_list = line.split(splitstr1)
    val tablename = line_list(0)
    val rowKey = line_list(1)
    val cols = line_list(2)
    val col = line_list(3)
    val value = line_list(4)
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(cols), Bytes.toBytes(col), Bytes.toBytes(value))
    (new ImmutableBytesWritable(Bytes.toBytes(tablename)), put)
  }

  def write_kafkaoffset(data:Map[String ,String]) = {
    //val data = Map("tablename"->offsettable,"group_id"->group_id,"topic"->o.topic.toString,"parts"->o.partition.toString,"offsets"->o.untilOffset.toString,"update_time"->NowDate())
    val clos = leizuname
    val tablename = data.get("tablename").mkString
    val topic = data.get("topic").mkString
    val parts = data.get("parts").mkString
    val rowkey = topic + parts + group_id
    val table = new HTable(hconf,tablename)
    val theput= new Put(Bytes.toBytes(rowkey))
    for(k <- data.keys.filter(_ != "tablename")) {
      theput.add(Bytes.toBytes(clos),Bytes.toBytes(k),Bytes.toBytes(data.get(k).mkString))
    }
    table.put(theput)
    table.close()
  }

  def get_kafka_offset(tablename:String,group_id: String, topic: String,parts:collection.mutable.Buffer[Int]):collection.mutable.Buffer[Map[String,String]] = {
    var rowkeys = collection.mutable.Buffer[String]()
    //以topicname+partitonid+group_id作为rowkey
    for(k <- parts) {rowkeys += topic + k.toString + group_id}
    val table = new HTable(hconf,tablename )
    var result_string_map = Map[String , String]()
    var finally_result = collection.mutable.Buffer[Map[String,String]]()
    for(rk <- rowkeys) {
      val theget= new Get(Bytes.toBytes(rk))
      val result=table.get(theget)
      val a = result.getFamilyMap((Bytes.toBytes(leizuname)))
      var ks = collection.mutable.Buffer[String]()
      if(a != null) {//可以从hbse中获取数据
        for (i <- a.keys) {
          result_string_map = result_string_map ++ Map(Bytes.toString(i) -> Bytes.toString(a.get(i)))
        }
        finally_result += result_string_map
      }
    }
    finally_result
  }

  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    return date
  }

  def iterator_fc(iterator:Iterator[String]):Iterator[String] = {
    var res = collection.mutable.Buffer[String]()
    while(iterator.hasNext) {
      val line = iterator.next
      val dict_data = Map("table" -> (process_json(line, "database") + ":" + process_json(line, "table")),
        "data" -> process_json(line, "data"), "diu_type_in_hbase" -> diu_type_in_hbase(process_json(line, "type")))
      val u_tablename = dict_data("table").toString
      //判断表是否需要同步及是否需要在hbase中建表
      if (need2synctables.contains(u_tablename)) {
        if (!hbase_exist_tables.contains(u_tablename)) {make_hbase_namespace_table(u_tablename)
          hbase_exist_tables += u_tablename }
        res  += line
      }
      else {
          if(! no_sync2hbase_tables.contains(u_tablename)) {
                    need2synctables = get_mysql_metadata(url, user, password)
                    if (need2synctables.contains(u_tablename)) {
                      if (!hbase_exist_tables.contains(u_tablename)) {make_hbase_namespace_table(u_tablename)}
                      res  += line }
                    else {no_sync2hbase_tables += u_tablename}
          }
      }
    }
    //返回一个迭代器，所有需要同步的数据
    return res.iterator
  }

  def format_fc(iterator: Iterator[String]):Iterator[String] = {
    var result_list = collection.mutable.Buffer[String]()
    while(iterator.hasNext) {
      val line = iterator.next
      val dict_data = Map("table" -> (process_json(line,"database") +":"+process_json(line,"table")),
        "data" -> process_json(line,"data"), "diu_type_in_hbase" -> diu_type_in_hbase(process_json(line,"type")))
      val u_tablename = dict_data("table").toString
      //格式化数据
      val myrowkey_cols = need2synctables(u_tablename).split(",")
      var rerowkey = collection.mutable.Buffer[String]()
      val jdata2 = new JSONObject(dict_data("data").toString)
      jdata2.put("diu_type_in_hbase",dict_data("diu_type_in_hbase").toString)
      val jdata2_keys = jdata2.keys()
      for(i <- myrowkey_cols){rerowkey += jdata2.getString(i)}
      //获取该条数据的rowkey
      val u_thisrowkey = rerowkey.mkString("_")
      //设置列簇
      val u_cols = leizuname

      while(jdata2_keys.hasNext) {
        val i = jdata2_keys.next
        // 表名，rowkey，列簇，列，值
        //使用splitstr1将list合并成字符串
        var datav :String = " "//防止有的值为null
        if(jdata2.getString(i.toString).mkString.length > 0) { datav = jdata2.getString(i.toString).mkString}
        //            println("datav: ",datav)
        val ii = List(u_tablename,u_thisrowkey,u_cols,i,datav).mkString(splitstr1)
        result_list.append(ii)
      }
      //使用splitstr2合并list
      result_list.mkString(splitstr2)
    }
    return result_list.iterator
  }

  def c2hbaseput(iterator: Iterator[String]):Iterator[(ImmutableBytesWritable, Put)] = {
    var result_list = collection.mutable.Buffer[(ImmutableBytesWritable, Put)]()
    while(iterator.hasNext) {
      val line = iterator.next
      val result = convertToHbasePut(line,splitstr1)
      result_list += result
    }
    return result_list.iterator
  }



  def main(args: Array[String]): Unit = {
    //获取上次消费的offset
    //从zk获取topic的partition数量
    val  zk = new ZooKeeper("192.168.188.80:2181,192.168.188.81:2181,192.168.188.82:2181",500,null)
    val s = zk.getChildren("/brokers/topics/maxwell2partitions/partitions",null)
    val numofpartitons = s.toArray().length

    var parts = collection.mutable.Buffer[Int]() //存放partiton编号
    var topicpartions =collection.mutable.Buffer[(String,Int)]() //[("maxwell",0),("maxwell",1),...] (topic,partition)
    var partiton_offset = collection.mutable.Buffer[(Int,Int)]() //[(0,123),(1,233)] (partition,offset)
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    for(i <- 0 to numofpartitons - 1) {
      parts += i
      topicpartions.append((topics.mkString, i))
    }

    val kafka_offset_info = get_kafka_offset(offsettable,group_id,topics.mkString,parts)
    if(kafka_offset_info.length > 0) { //可以从hbase获取到offset
      for (i <- 0 to kafka_offset_info.length - 1) {
        if (kafka_offset_info(i).get("offsets").mkString.toInt > 0) {
          partiton_offset.append((i, kafka_offset_info(i).get("offsets").mkString.toInt))
        } else {
          partiton_offset.append((i, 0))
        }
      }
      for(i <- 0 to numofpartitons - 1) {
        fromOffsets += (TopicAndPartition(topicpartions(i)._1,topicpartions(i)._2) -> partiton_offset(i)._2.toLong)
      }
    }
    val offsetnumbers = for(i <- partiton_offset)  yield i._2
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(1))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")

    var messages: InputDStream[(String, String)] = null
    if (offsetnumbers.sum > 0) {
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {//无法从hbase中获取数据时
      messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    val lines = messages.map(_._2).filter(line => line.length > 5)
    //检查是否需同步并判断habse中是否需建表
    val lines2 = lines.mapPartitions(iterator_fc).filter(line => line.length > 5)
    //格式化字符串
    val lines3 = lines2.mapPartitions(format_fc).flatMap(line =>{line.toString.split(splitstr2)}).filter(line => line.length > 5)
    //转换成hbaseput
    val result = lines3.mapPartitions(c2hbaseput)
    //写入hbase
    result.foreachRDD(rdd => {if(! rdd.isEmpty()){rdd.saveAsNewAPIHadoopFile("/user/bigdata", classOf[ImmutableBytesWritable], classOf[Put], classOf[MultiTableOutputFormat], jobConf)}})
    var offsetRanges = Array.empty[OffsetRange]
    messages.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map(_._2).foreachRDD { rdd =>
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset} ${NowDate()} ")
        val data = Map("tablename"->offsettable,"group_id"->group_id,"topic"->o.topic.toString,"parts"->o.partition.toString,"offsets"->o.untilOffset.toString,"update_time"->NowDate())
        write_kafkaoffset(data) //offset写入hbase
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
