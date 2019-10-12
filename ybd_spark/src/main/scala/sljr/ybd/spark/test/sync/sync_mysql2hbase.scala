package sljr.ybd.spark.test.sync

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka._
import org.codehaus.jettison.json._



object write2hbase_test20171226 {
  //mysql配置信息
  val url = "jdbc:mysql://192.168.188.80/yfgtest"
  val user = "test"
  val password = "123456"
  val offsettable = "yfgtest.streaming_task2"
  val diu_type_in_hbase = Map("delete" -> 0, "insert" -> 1, "update" -> 2)

  var need2synctables = Map[String,String]()
  need2synctables = get_mysql_metadata(url, user, password)

  //  var no2synctables = collection.mutable.Buffer[String]()
  var hbase_exist_tables = collection.mutable.Buffer[String]()
  hbase_exist_tables = get_all_hbsae_tables()

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
    val zkips = "192.168.188.80,192.168.188.81,192.168.188.82"
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

  def make_hbase_namespace_table(htablename: String) = {
    val hconnection = get_hbsae_connection()
    val admin = hconnection.getAdmin()
    val namespace = htablename.split(":")(0).toString
    val tablename = htablename.split(":")(1).toString
    val namespace_list = get_hbase_namespaces()
    val tablename_list = get_all_hbsae_tables()
    //命名空间不存在，则创建命名空间
    if (!namespace_list.contains(namespace)) {
      admin.createNamespace(NamespaceDescriptor.create(namespace).build())
    }
    //准备建表，设置表名
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(htablename))
    //设置列簇名
    val hColumnDescriptor = new HColumnDescriptor("data")
    //将列簇设置加入HTableDescriptor
    tableDescriptor.addFamily(hColumnDescriptor)
    //建表
    admin.createTable(tableDescriptor)
    admin.close()
  }

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
    //    println(line)
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

  def write_kafkaoffset2mysql(url: String, user: String, password: String, data: Map[String, String]) = {
    //data  {"tablename":"yfgtest.streaming_task2","group_id":"test2hbase","topic":"maxwell","parts":"0","offsets":"12346","update_time":"2017-12-26 09:39:39"}
    val driver = "com.mysql.jdbc.Driver"
    val tablename = data("tablename").mkString
    val group_id = "'" + data("group_id").mkString + "'"
    val topic = "'" + data("topic").mkString + "'"
    val parts = "'" + data("parts").mkString + "'"
    val offsets = "'" + data("offsets").mkString + "'"
    val update_time = "'" + data("update_time").mkString + "'"
    val sQLStatement = s"INSERT INTO ${tablename}(group_id,topic,parts,offsets,update_time) " +
      s"VALUES(${group_id},${topic},${parts},${offsets},${update_time}) ON DUPLICATE KEY UPDATE " +
      s"group_id=${group_id},topic=${topic},parts=${parts},offsets=${offsets},update_time=${update_time}"
    var connection: java.sql.Connection = null
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, password)
    val statement = connection.createStatement()
    val resultSet = statement.execute(sQLStatement)
    connection.close()
  }

  def get_kafka_offset(url: String, user: String, password: String, group_id: String, topic: String): Map[String, String] = {
    val driver = "com.mysql.jdbc.Driver"
    var tb_key_cols: Map[String, String] = Map()
    val sQLStatement = s"SELECT topic,parts,offsets FROM yfgtest.streaming_task2 where group_id='${group_id}' and topic = '${topic}';"
    var connection: java.sql.Connection = null
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, password)
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sQLStatement)
    while (resultSet.next()) {
      tb_key_cols += ("topic" -> resultSet.getString("topic"))
      tb_key_cols += ("parts" -> resultSet.getString("parts"))
      tb_key_cols += ("offsets" -> resultSet.getString("offsets"))
    }
    connection.close()
    return tb_key_cols
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
      if (need2synctables.contains(dict_data("table").toString)) {
        if (!hbase_exist_tables.contains(u_tablename)) {make_hbase_namespace_table(u_tablename)
          hbase_exist_tables += u_tablename }
        res  += line
      }
      else {need2synctables = get_mysql_metadata(url, user, password)
        if (need2synctables.contains(dict_data("table").toString)) {
          if (!hbase_exist_tables.contains(u_tablename)) {make_hbase_namespace_table(u_tablename)}
          res  += line
        }
      }
    }
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
      val myrowkey = for(i <- myrowkey_cols){rerowkey += jdata2.getString(i)}
      //获取该条数据的rowkey
      val u_thisrowkey = rerowkey.mkString("_")
      //设置列簇
      val u_cols = "data"

      while(jdata2_keys.hasNext) {
        val i = jdata2_keys.next
        // 表名，rowkey，列簇，列，值
        //使用splitstr1将list合并成字符串
        var datav :String = " "
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
    var start_offset: String = ""
    val kafka_offset_info = get_kafka_offset(url, user, password, group_id, topic = topics.mkString)
    if (kafka_offset_info.size > 0) {
      start_offset = kafka_offset_info("offsets")
    }
    else {
      start_offset = "0"
    }

    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(1))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")
    val diu_type_in_hbase = Map("delete" -> 0, "insert" -> 1, "update" -> 2)

    val parttiton = 0
    val topicpartion = TopicAndPartition(topics.mkString, parttiton)
    val fromoffset = Map(topicpartion -> start_offset.toLong)

    var messages: InputDStream[(String, String)] = null
    if (start_offset.toLong > 0) {
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromoffset, messageHandler)
    } else {
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
    result.foreachRDD(rdd => {if(! rdd.isEmpty()){rdd.saveAsNewAPIHadoopFile("/user/yfgtest", classOf[ImmutableBytesWritable], classOf[Put], classOf[MultiTableOutputFormat], jobConf)}})
    var offsetRanges = Array.empty[OffsetRange]
    messages.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map(_._2).foreachRDD { rdd =>
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset} ${NowDate()}")
        val data = Map("tablename"->offsettable,"group_id"->group_id,"topic"->o.topic.toString,"parts"->o.partition.toString,"offsets"->o.untilOffset.toString,"update_time"->NowDate())
        write_kafkaoffset2mysql(url,user,password,data)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}


