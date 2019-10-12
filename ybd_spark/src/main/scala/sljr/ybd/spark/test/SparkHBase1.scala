package sljr.ybd.spark.test
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client.Get
import org.apache.spark.serializer.KryoSerializer

object SparkHBase1 extends Serializable {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("HBaseTest")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    val sc = new SparkContext(sparkConf)
    var table_name = "test"
    val conf = HBaseConfiguration.create()

    conf.set("hbase.rootdir", "hdfs://wwwwww-1/hbase")
    conf.set("hbase.zookeeper.quorum", "11.11.131.19,11.11.131.20,11.11.131.21")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.master", "60001")
    conf.set(TableInputFormat.INPUT_TABLE, table_name)

    val hadmin = new HBaseAdmin(conf)

    if (!hadmin.isTableAvailable("test")) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor("test")
      tableDesc.addFamily(new HColumnDescriptor("basic".getBytes()))
      hadmin.createTable(tableDesc)
    }else{
      print("Table  Exists!  not Create Table")
    }

    val table = new HTable(conf, "test");
    for (i <- 1 to 5) {
      var put = new Put(Bytes.toBytes("row" + i))
      put.add(Bytes.toBytes("basic"), Bytes.toBytes("name"), Bytes.toBytes("value " + i))
      table.put(put)
    }
    table.flushCommits()

    //Scan操作
    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hbaseRDD.count()
    println("HBase RDD Count:" + count)
    hbaseRDD.cache()

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val g = new Get("row1".getBytes)
    val result = table.get(g)
    val value = Bytes.toString(result.getValue("basic".getBytes,"name".getBytes))
    println("GET id001 :"+value)


    hbaseRDD.cache()
    print("------------------------scan----------")
    val res = hbaseRDD.take(count.toInt)
    for (j <- 1 until count.toInt) {
      println("j: " + j)
      var rs = res(j - 1)._2
      var kvs = rs.raw
      for (kv <- kvs)
        println("rowkey:" + new String(kv.getRow()) +
          " cf:" + new String(kv.getFamily()) +
          " column:" + new String(kv.getQualifier()) +
          " value:" + new String(kv.getValue()))
    }

    /*    println("-------------------------")
        println("--take1" + hBaseRDD.take(1))
        println("--count" + hBaseRDD.count())*/


    //insert_hbase(100002,3)
  }
  //写入hbase
  /* def insert_hbase(news_id:Int,type_id:Int): Unit ={
     var table_name = "news"
     val conf = HBaseConfiguration.create()
     conf.set("hbase.zookeeper.quorum","192.168.110.233, 192.168.110.234, 192.168.110.235");
     conf.set("hbase.zookeeper.property.clientPort", "2181");
     val table = new HTable(conf, table_name)
     val hadmin = new HBaseAdmin(conf)
     val row = Bytes.toBytes(news_id.toString())
     val p = new Put(row)
     p.add(Bytes.toBytes("content"),Bytes.toBytes("typeid"),Bytes.toBytes(type_id.toString()))
     table.put(p)
     table.close()
   } */
}