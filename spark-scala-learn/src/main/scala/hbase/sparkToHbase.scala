package hbase
//
//import kafka.PropertiesScalaUtils
//import net.sf.json.JSONObject
//import org.apache.hadoop.hbase.client.{Put, Result}
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.mapreduce.Job
//import org.apache.spark.SparkConf
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
//import spark.wordcount.kafkaStreams
//
///**
//  * sparkstreaming写入hbase新的API;
//  */
//object sparkToHbase {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("Hbase Test")
//    val scc = new StreamingContext(conf, Seconds(1))
//    val sc = scc.sparkContext
//    val tablename = "test"
//    val mode = args(0).toString
//    val zk_hbase = PropertiesScalaUtils.loadProperties("zk_hbase",mode)
//    val zk_port = PropertiesScalaUtils.loadProperties("zk_port",mode)
//    val hbase_master = PropertiesScalaUtils.loadProperties("hbase_master",mode)
//    val hbase_rootdir = PropertiesScalaUtils.loadProperties("hbase_rootdir",mode)
//    val zookeeper_znode_parent = PropertiesScalaUtils.loadProperties("zookeeper_znode_parent",mode)
//    val topic = PropertiesScalaUtils.loadProperties("topic_combine",mode)
//    val broker = PropertiesScalaUtils.loadProperties("broker",mode)
//    sc.hadoopConfiguration.set("hbase.zookeeper.quorum",zk_hbase)
//    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", zk_port)
//    sc.hadoopConfiguration.set("hbase.master", hbase_master)
//    sc.hadoopConfiguration.set("hbase.defaults.for.version.skip", "true")
//    sc.hadoopConfiguration.set("hhbase.rootdir", hbase_rootdir)
//    sc.hadoopConfiguration.set("zookeeper.znode.parent", zookeeper_znode_parent)
//    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)
//    val job = Job.getInstance(sc.hadoopConfiguration)
//    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
//    job.setOutputValueClass(classOf[Result])
//    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
//    val topicSet = Set(topic)
//    val kafkaParams = Map[String, Object](
//      "auto.offset.reset" -> "latest",   //latest;earliest
//      "value.deserializer" -> classOf[StringDeserializer] //key,value的反序列化;
//      , "key.deserializer" -> classOf[StringDeserializer]
//      , "bootstrap.servers" -> broker
//      , "group.id" -> "jason_test"
//      , "enable.auto.commit" -> (true: java.lang.Boolean)
//    )
//    kafkaStreams = KafkaUtils.createDirectStream[String, String](
//      scc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams))
//    try {
//      kafkaStreams.foreachRDD(rdd => {
//        if(!rdd.isEmpty()){
//          val save_rdd = rdd.map(x => {
//            val json = JSONObject.fromObject(x.value())
//            val put = new Put(Bytes.toBytes(json.get("rowkey").toString))
//            insert_hb(json,put)
//            (new ImmutableBytesWritable, put)
//          })
//          save_rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
//        }
//      })
//    }catch {
//      case e:Exception => println("报错了")
//    }
//    scc.start()
//    scc.awaitTermination()
//  }
//  def insert_hb(json: JSONObject, onePut: Put): Unit = {
//    val keys = json.keySet
//    val iterator_redis = keys.iterator
//    while (iterator_redis.hasNext) {
//      val hb_col = iterator_redis.next().toString
//      val col_value = json.get(hb_col).toString
//      onePut.addColumn(Bytes.toBytes("f1"), Bytes.toBytes(hb_col), Bytes.toBytes(col_value))
//    }
//  }
//}