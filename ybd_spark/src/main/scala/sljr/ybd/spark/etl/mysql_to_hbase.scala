package sljr.ybd.spark.etl
import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}
import java.sql.{Connection, DriverManager}
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.{Duration, Seconds}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection , ConnectionFactory}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

object mysql_to_hbase {
  @transient
  def get_mysql_conn(ip:String ,database: String,port:String,username:String ,password:String): java.sql.Connection ={
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://"+ip+":"+port+"/"+database
    var connection:java.sql.Connection = null
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      println(ip+" 连接成功")
    } catch {
        case e => e.printStackTrace
        println(ip+" 连接失败")
      //case _: Throwable => println("ERROR")
    }
    return connection
  }
  @transient
  def get_mysql_meta(connection: java.sql.Connection ): Map[String,String] ={
    var tb_key_cols:Map[String,String] = Map()
    try {
        val statement = connection.createStatement()
        val sql="select concat(TABLE_SCHEMA,':',TABLE_NAME) tb,GROUP_CONCAT(COLUMN_NAME) key_cols" +
        " from information_schema.key_column_usage where CONSTRAINT_NAME='PRIMARY' and TABLE_SCHEMA<>'mysql' " +
        " GROUP BY TABLE_SCHEMA ,TABLE_NAME"
        val resultSet = statement.executeQuery(sql)
        while ( resultSet.next() ) {
        tb_key_cols+=(resultSet.getString("tb") ->  resultSet.getString("key_cols"))
        }
     } catch {
          case e => e.printStackTrace
      //case _: Throwable => println("ERROR")
     }
    return tb_key_cols
  }
  def src_mysql_tb_key(mysql_host:String ,database: String,mysql_port:String,mysql_user:String ,mysql_password:String): Map[String,String] ={
    var tb_key_cols:Map[String,String] = Map()
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://"+mysql_host+":"+mysql_port+"/"+database
    var connection:java.sql.Connection  = null
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, mysql_user, mysql_password)
      println(mysql_host+" 连接成功")
      val statement = connection.createStatement()
      val sql="select concat(TABLE_SCHEMA,':',TABLE_NAME) tb,GROUP_CONCAT(COLUMN_NAME) key_cols" +
        " from information_schema.key_column_usage where CONSTRAINT_NAME='PRIMARY' and TABLE_SCHEMA<>'mysql' " +
        " GROUP BY TABLE_SCHEMA ,TABLE_NAME"
      val resultSet = statement.executeQuery(sql)
      while ( resultSet.next() ) {
        tb_key_cols += (resultSet.getString("tb") -> resultSet.getString("key_cols"))
      }
    } catch {
      case e => e.printStackTrace
        println(mysql_host+" 连接失败")
    }
    return tb_key_cols
  }
  @transient
  def str_to_json(topare:String) :Tuple2[String,String]={
      var json=JSON.parseObject(topare)
      val diu_type_in_hbase = Map("delete"->"0" , "insert"->"1", "update"->"2")
      var hbase_tbale=json.get("database").toString+":"+json.get("table").toString
      var types=diu_type_in_hbase(json.get("type").toString)
      var datas=json.getJSONObject("data")
      val jsonKey=datas.keySet()//.forEach(x=>print(x))
      val iter = jsonKey.iterator
    return ("",datas.toString())
  }
  @transient
  def get_offsets(connection: java.sql.Connection ,group_id:String ): Map[TopicAndPartition, Long] ={
    var topic_part_offsets:Map[TopicAndPartition, Long] = Map()
    try {
      val statement = connection.createStatement()
      val sql="SELECT topic,parts,offsets FROM streaming_task where group_id='"+group_id+"';"
      val resultSet = statement.executeQuery(sql)
      while ( resultSet.next() ) {
        topic_part_offsets += (new TopicAndPartition(resultSet.getString("topic"),resultSet.getInt("parts")) ->  resultSet.getLong("offsets") )
      }
    } catch {
      case e => e.printStackTrace
    }
    return topic_part_offsets
  }

  def createHTable(tablename: String): Unit= {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "cdh1,cdh2,cdh3");
    conf.set(TableInputFormat.INPUT_TABLE, "yfgtest:user")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val hbase_conn = ConnectionFactory.createConnection(conf)
    val hbase_admin = hbase_conn.getAdmin()
    //Hbase表模式管理器
    //val admin = hbase_conn.getAdmin
    //本例将操作的表名
    val tableName = TableName.valueOf(tablename)
    //如果需要创建表
    if (!hbase_admin.tableExists(tableName)) {
      //创建Hbase表模式
      val tableDescriptor = new HTableDescriptor(tableName)
      //创建列簇1
      tableDescriptor.addFamily(new HColumnDescriptor("datas".getBytes()))
      //创建表
      hbase_admin.createTable(tableDescriptor)
      println("create done.")
    }
  }
  def main(args: Array[String]): Unit = {
    var mysql_host = "192.168.188.80"
    var mysql_port ="3306"
    var mysql_user = "test"
    var mysql_password = "123456"
    var database= "yfgtest"
    @transient
    var source_mysql_conn=get_mysql_conn(mysql_host,database,mysql_port,mysql_user,mysql_password) // 同步的原数据连接
    @transient
    var offset_mysql_conn=get_mysql_conn( "192.168.188.80","yfgtest","3306", "test","123456") // offfset存储的mysql库
    var tb_key_clos_map=get_mysql_meta(source_mysql_conn)
    var zk_hosts = "cdh1,cdh2,cdh3" //
    var group_id="maxwell_31"
    val kafkaoffset_hbase_table =group_id+"kafka_offset"
    var topic =  Set("maxwell")
    var brokers = "cdh2:9092,cdh3:9092"
    var topic_parts_offsets=get_offsets(offset_mysql_conn: java.sql.Connection ,group_id )
    var sparkConf = new SparkConf().setMaster("local[4]").setAppName("mysql_to_hbase")
    var ssc = new StreamingContext(sparkConf, Seconds(5))
    var broad_tb_key_cols = ssc.sparkContext.broadcast(tb_key_clos_map)
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","cdh1,cdh2,cdh3");
    conf.set(TableInputFormat.INPUT_TABLE, kafkaoffset_hbase_table)
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    val hbase_conn = ConnectionFactory.createConnection(conf);
    val hbase_admin = hbase_conn.getAdmin()
    //var sync_hbase=new sync_hbase(zk_hosts,kafkaoffset_hbase_table)
    @transient
    val diu_type_in_hbase = Map("delete"->"0" , "insert"->"1", "update"->"2")
    // println(broad_tb_key_cols.value.contains("hue:oozie_fs"))
    var kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "zookeeper.connect" -> zk_hosts,
      "serializer.class" -> "kafka.serializer.StringEncoder",
       "group.id" -> group_id,
       "auto.offset.reset" -> "smallest"
    )
   println("开始---------------")
    var kafkaStream: InputDStream[(String, String)] =null
    var offsetRanges = Array[OffsetRange]()
    // topic_parts_offsets += (new TopicAndPartition("maxwell",0 )->  1258779)
    if(topic_parts_offsets.size>0){ //存在 offset 记录
      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, topic_parts_offsets, messageHandler)
    }
    else{
      println("没有offset,初始化连接")
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic)
    }
    kafkaStream.transform {
          rdd=>{
               offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
               rdd
              }
        }.map(_._2).foreachRDD(rdd=>{
            for (o <- offsetRanges) {
                  print("执行的offsetrange:")
                  println(o)
                  }
        print("打印 sdd:")
        if(!rdd.isEmpty()){
            println("有数值rdd")
            println(rdd.first())
            rdd.map(x=>{
              if(x.contains("data")) {
                var json = JSON.parseObject(x)
                var hbase_table = json.get("database").toString + ":" + json.get("table").toString
                var types = diu_type_in_hbase(json.get("type").toString)
                var datas = json.getJSONObject("data")
                if (broad_tb_key_cols.value.contains(hbase_table)) {
                  var key_cols = broad_tb_key_cols.value(hbase_table)
                  var row_keys = ""
                  for (col <- key_cols.split(",")) {
                    row_keys = row_keys + datas.get(col).toString
                  }
                  println(row_keys)
                } else {
                  println(hbase_table+"表不存在")
                  val tb_key_cols =src_mysql_tb_key(mysql_host ,database,mysql_port,mysql_user ,mysql_password)
                  broad_tb_key_cols.unpersist() // 删除广播变量并重新广播
                  broad_tb_key_cols = ssc.sparkContext.broadcast(tb_key_cols)
                  /*var tb_key_cols:Map[String,String] = Map()
                  val driver = "com.mysql.jdbc.Driver"
                  val url = "jdbc:mysql://"+mysql_host+":"+mysql_port+"/"+database
                  var connection:java.sql.Connection  = null
                  try {
                    Class.forName(driver)
                    connection = DriverManager.getConnection(url, mysql_user, mysql_password)
                    println(mysql_host+" 连接成功")
                    val statement = connection.createStatement()
                    val sql="select concat(TABLE_SCHEMA,':',TABLE_NAME) tb,GROUP_CONCAT(COLUMN_NAME) key_cols" +
                      " from information_schema.key_column_usage where CONSTRAINT_NAME='PRIMARY' and TABLE_SCHEMA<>'mysql' " +
                      " GROUP BY TABLE_SCHEMA ,TABLE_NAME"
                    val resultSet = statement.executeQuery(sql)
                    while ( resultSet.next() ) {
                      tb_key_cols += (resultSet.getString("tb") -> resultSet.getString("key_cols"))
                    }
                  } catch {
                    case e => e.printStackTrace
                      println(mysql_host+" 连接失败")
                  }
                  broad_tb_key_cols.unpersist() // 删除广播变量并重新广播
                  broad_tb_key_cols = ssc.sparkContext.broadcast(tb_key_cols)
                  //hbase_admin.tableExists(TableName.valueOf(hbase_table))
                  //createHTable(hbase_table)*/
                 }
              }else{println("字符串不合法")}
            })
          }else{
            println("rdd空值")
          }
        })

    /*kafkaStream.map(_._2).map(line => {
      val json = parse(line)
      //json.extract[LogStashV1]
      // println("json")
    }).print()*/
    ssc.start()
    ssc.awaitTermination()
  }
}