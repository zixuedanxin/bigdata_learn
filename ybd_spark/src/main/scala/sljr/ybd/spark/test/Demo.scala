package sljr.ybd.spark.test
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{ HBaseConfiguration, HColumnDescriptor, HTableDescriptor }
import org.apache.hadoop.hbase.client.{ HBaseAdmin, HTable, Put }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.datasources.hbase
import org.apache.hadoop.hbase.spark.datasources.HBaseScanPartition
import org.apache.hadoop.hbase.util.Bytes

case class HBaseRecord(
                        col0: String,
                        col1: Int)

object HBaseRecord {
  def apply(i: Int, t: Int): HBaseRecord = {
    val s = s"""row${"%03d".format(i)}"""
    HBaseRecord(s,
      i)
  }
}

object Demo {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("test spark sql");
    conf.setMaster("spark://master:7077");
    val sc = new SparkContext("local", "test") //new SparkContext(conf)//
    val config = HBaseConfiguration.create()
    //config.addResource("/home/hadoop/hbase-1.2.2/conf/hbase-site.xml");
    //config.set("hbase.zookeeper.quorum", "node1,node2,node3");
    val hbaseContext = new HBaseContext(sc, config, null)

    def catalog = s"""{
                     |"table":{"namespace":"default", "name":"table4"},
                     |"rowkey":"key",
                     |"columns":{
                     |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                     |"col1":{"cf":"cf1", "col":"col1", "type":"int"}
                     |}
                     |}""".stripMargin

    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog -> cat))
        .format("org.apache.hadoop.hbase.spark")
        .load()
    }
    val df = withCatalog(catalog)

    val res = df.select("col1")
    //res.save("hdfs://master:9000/user/yang/a.txt")
    res.show()
    df.registerTempTable("table4")
    sqlContext.sql("select count(col0),sum(col1) from table4 where col1>'20' and col1<'26' ").show
    println("-----------------------------------------------------");
    sqlContext.sql("select count(col1),avg(col1) from table4").show
  }
}

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE,HBASE_TABLE )
    conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()))
    sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
      //后面是针对hbase查询结果的具体业务逻辑
      .map()
    ...

    def main(args: Array[String]): Unit = {
      val Array(output_path) = args

      val sparkConf = new SparkConf().setAppName("demo")
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val sc = new SparkContext(sparkConf)

      genUuidWifi(sc).saveAsTextFile(output_path)
      sc.stop()
    }
  }