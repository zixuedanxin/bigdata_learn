package sljr.ybd.spark.test.hbase
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{BufferedMutator, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import java.util.concurrent.Executors
import org.apache.hadoop.security.UserGroupInformation;
/**
  * Document:本类作用---->获取Hbase连接
  * User: yangjf
  * Date: 2016/8/18  17:40
  */
object HbaseConnectionUtil {
  val myConf = HBaseConfiguration.create()
  //常规方式获取链接
  def  getHbaseConn( ipStr:String) :HBaseAdmin= {
    //获取配置
    val conf=HBaseConfiguration.create();
    //ipstr="192.168.142.115,192.168.142.116,192.168.142.117"---->以逗号分隔
    conf.set("hbase.zookeeper.quorum",ipStr);
    //获取Hbase的master
    val admin=new HBaseAdmin(conf);
    admin
  }
  //释放连接
  def releaseConn( admin:HBaseAdmin) :Unit= {
    try{
      if(admin!=null){
        admin.close();
      }
    }catch{
      case ex:Exception=>ex.getMessage
    }
  }



  def  getHbaseConn(): Connection = {
    //      val hbaseConn = ConnectionFactory.createConnection(myConf);
    //      val mutator = hbaseConn.getBufferedMutator(TableName.valueOf("tableName"))
    myConf.set("hbase.zookeeper.quorum", "192.168.1.31,192.168.1.32,192.168.1.13")
    myConf.set("hbase.zookeeper.property.clientPort", "2181")
    myConf.set("hbase.regionserver.thread.compaction.large", "5")
    myConf.set("hbase.regionserver.thread.compaction.small", "5")
    //    myConf.set("hbase.hregion.majorcompaction","0")
    myConf.set("hbase.hstore.compaction.min", "10")
    myConf.set("hbase.hstore.compaction.max", "10")
    myConf.set("hbase.hstore.blockingStoreFiles", "100")
    myConf.set("hbase.hstore.compactionThreshold", "7")
    myConf.set("hbase.regionserver.handler.count", "100")
    myConf.set("hbase.regionserver.hlog.splitlog.writer.threads", "10")
    myConf.set("hbase.regionserver.thread.compaction.small", "5")
    myConf.set("hbase.regionserver.thread.compaction.large", "8")
    myConf.set("hbase.hregion.max.filesizes", "4G")
    myConf.set("hbase.hregion.max.filesize", "60G")
    myConf.set("hbase.hregion.memstore.flush.size", "60G")
    myConf.set("hbase.hregion.memstore.block.multiplier", "5")
    //    myConf.set("hbase.client.write.buffer", "8388608")
    myConf.set("hbase.client.pause", "200")
    myConf.set("hbase.client.retries.number", "71")
    myConf.set("hbase.ipc.client.tcpnodelay", "false")
    myConf.set("hbase.client.scanner.caching", "500")
    myConf.set("hbase.htable.threads.max", Integer.MAX_VALUE.toString)
    myConf.set("hbase.htable.threads.keepalivetime", "60")
    val threadPool = Executors.newFixedThreadPool(Runtime.getRuntime()
      .availableProcessors())
    ConnectionFactory.createConnection(myConf, threadPool)
    //    ConnectionFactory.createConnection(myConf)
  }

}