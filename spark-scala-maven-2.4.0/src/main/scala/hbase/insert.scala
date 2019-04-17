package hbase
import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{LoadIncrementalHFiles, MultiTableOutputFormat, TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import SparkContext._
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.spark.sql.SparkSession


object insert extends Serializable {
  //获取hbase链接
  def get_hbsae_connection(zkips: String): org.apache.hadoop.hbase.client.Connection = {
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", zkips)
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    val hconnection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(hconf)
    return hconnection
  }
  //获取hbsae所有表
  def get_all_hbsae_tables(zkips: String): collection.mutable.Buffer[String] = {
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
  def main(args: Array[String]): Unit = {

    val zkips = "localhost"
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", zkips)
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    var hbase_exist_tables = collection.mutable.Buffer[String]()
    hbase_exist_tables = get_all_hbsae_tables(zkips)
    println(hbase_exist_tables)
    val hbase_conn=get_hbsae_connection(zkips)
    val tb=hbase_conn.getTable(TableName.valueOf("test2"))
    val get = new Get(Bytes.toBytes("rowkey1"))
    val res = tb.get(get)
    println(res.rawCells())
    val spark =
      SparkSession.builder()
        .appName("Dataset-Basic")
        .master("local[4]")
        .getOrCreate()
    // val sparkConf = new SparkConf().setMaster("localhost").setAppName("lxw1234.com")
    val sc =spark.sparkContext // new SparkContext(sparkConf);
    val rdd1 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)))
    // rdd1.saveAsNewAPIHadoopFile("/tmp/test",classOf[Text],classOf[IntWritable],classOf[TextOutputFormat[Text,IntWritable]])
    val jobConf = new JobConf(hconf, this.getClass)
    jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
    jobConf.setOutputValueClass(classOf[Put])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"test2")
    jobConf.setOutputFormat(classOf[org.apache.hadoop.hbase.mapred.TableOutputFormat])
    println(jobConf.get("zookeeper.znode.parent"))

    rdd1.map(
      x => {
        var put = new Put(Bytes.toBytes(x._1))
        put.addColumn(Bytes.toBytes("family1"), Bytes.toBytes("c3"), Bytes.toBytes(x._2))
        (new ImmutableBytesWritable,put)
      }
    ).saveAsHadoopDataset(jobConf)

     
     sc.hadoopConfiguration.set("hbase.zookeeper.quorum","localhost")
     sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
     sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "test2")
     val job = Job.getInstance(sc.hadoopConfiguration) // 过时用法 ： new Job(sc.hadoopConfiguration)
     job.setOutputKeyClass(classOf[ImmutableBytesWritable])
     job.setOutputValueClass(classOf[Result])
     //job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]]) // TableOutputFormat  存在两个mapreduce mapre
      job.setOutputFormatClass(classOf[MultiTableOutputFormat])           // 插入 不同的表

    rdd1.map(
          x => {
        val put = new Put(Bytes.toBytes(x._1))
        put.addColumn(Bytes.toBytes("family2"), Bytes.toBytes("c4"), Bytes.toBytes(x._2))
        (new ImmutableBytesWritable(Bytes.toBytes("test3")),put)     //(new ImmutableBytesWritable(),put)
      }
    ).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

}
