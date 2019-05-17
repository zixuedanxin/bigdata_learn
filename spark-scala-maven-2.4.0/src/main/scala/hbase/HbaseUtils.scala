package hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer


class HbaseUtils {

  /**
    * 获取管理员对象
    *
    * @param conf 对hbase client配置一些参数
    * @return 返回hbase的HBaseAdmin管理员对象
    */
  def getAdmin(conf: Configuration): HBaseAdmin = {
    val conn = ConnectionFactory.createConnection(conf)
    conn.getAdmin().asInstanceOf[HBaseAdmin]
  }

  /**
    * 根据指定的管理员，表名，列族名称创建表
    *
    * @param admin         创建HBaseAdmin对象
    * @param tName         需要创建的表名
    * @param columnFamilys 列族名称的集合
    */
  def createTable(admin: HBaseAdmin, tName: String, columnFamilys: List[String]): Unit = {
    if (admin.tableExists(TableName.valueOf(tName))) {
      println("table already exists!")
      admin.disableTable(tName)
      admin.deleteTable(tName)
    }
    try {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tName))
      columnFamilys.foreach(columnFamilysName => tableDesc.addFamily(new HColumnDescriptor(columnFamilysName)))
      admin.createTable(tableDesc)
      println("create table success!")
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 单条数据插入 根据表名、rowkey、列族名、列名、值、增加数据
    *
    * @param conf         当前对象的配置信息
    * @param tableName    表名
    * @param rowKey       行键
    * @param columnFamily 列族名称
    * @param column       列
    * @param value        值
    */
  def insertData(conf: Configuration, tableName: String, rowKey: String, columnFamily: String, column: String, value: String): Unit = {

    val con = ConnectionFactory.createConnection(conf)
    val table = con.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value))
    table.put(put)
    close(table, con)
    print("数据插入成功")
  }

  /**
    * 批量插入数据
    *
    * @param conf         当前对象的配置信息
    * @param tableName    表名
    * @param rowKey       行键
    * @param columnFamily 列族
    * @param column       列
    * @param value        值
    */
  def batchInsertData(conf: Configuration, tableName: String, rowKey: String, columnFamily: String, column: String, value: String): Unit = {
    val con = ConnectionFactory.createConnection(conf)
    val table: BufferedMutator = con.getBufferedMutator(TableName.valueOf(tableName))
    val p = new Put(Bytes.toBytes(rowKey))
    p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value))
    val mutations = new java.util.ArrayList[Mutation]()
    mutations.add(p)
    table.mutate(mutations)
    table.flush()
    if (con != null)
      con.close()
    if (table != null)
      table.close()
    print("数据插入成功")
  }

  /**
    * 删除数据
    *
    * @param conf      当前对象的配置信息
    * @param tableName 表名
    */
  def deleteData(conf: Configuration, tableName: String): Unit = {
    val admin = getAdmin(conf)
    try {
      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    print("删除数据成功")
  }

  /**
    * 根据指定的配置信息全表扫描指定的表
    *
    * @param conf      配置信息
    * @param tableName 表名
    * @return Cell单元格数组
    */
  def getByScan(conf: Configuration, tableName: String): ArrayBuffer[Array[Cell]] = {
    var arrayBuffer = ArrayBuffer[Array[Cell]]()
    val scanner = new Scan()
    val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf(tableName))
    val results = table.getScanner(scanner)
    var res: Result = results.next()
    while (res != null) {
      arrayBuffer += res.rawCells()
      res = results.next()
    }
    arrayBuffer
  }

  /**
    * 根据行键获取具体的某一个行
    *
    * @param conf      配置信息
    * @param tableName 表名
    * @param row       行键
    * @return Array[Cell]
    */
  def getRow(conf: Configuration, tableName: String, row: String): Array[Cell] = {
    val con = ConnectionFactory.createConnection(conf)
    val table = con.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(row))
    val res = table.get(get)
    res.rawCells()
  }

  /**
    * 删除指定表的指定row数据
    *
    * @param conf      配置信息
    * @param tableName 表名
    * @param row       行键
    */
  def delRow(conf: Configuration, tableName: String, row: String): Unit = {
    val con = ConnectionFactory.createConnection(conf)
    val table = con.getTable(TableName.valueOf(tableName))
    table.delete(new Delete(Bytes.toBytes(row)))
    println("删除数据成功")
  }

  def close(table: Table, con: Connection): Unit = {
    if (table != null)
      table.close()
    if (con != null)
      con.close()
  }

}

//public class HBaseUtils {
//
//  private static final String QUORUM = "192.168.1.100";
//  private static final String CLIENTPORT = "2181";
//  private static Configuration conf = null;
//  private static HConnection conn = null;
//
//  /**
//    * 获取全局唯一的Configuration实例
//    * @return
//    */
//  public static synchronized Configuration getConfiguration()
//  {
//    if(conf == null)
//    {
//      conf =  HBaseConfiguration.create();
//      conf.set("hbase.zookeeper.quorum", QUORUM);
//      conf.set("hbase.zookeeper.property.clientPort", CLIENTPORT);
//    }
//    return conf;
//  }
//
//  /**
//    * 获取全局唯一的HConnection实例
//    * @return
//    * @throws ZooKeeperConnectionException
//    */
//  public static synchronized HConnection getHConnection() throws ZooKeeperConnectionException
//  {
//    if(conn == null)
//    {
//      /*
//       * * 创建一个HConnection
//       * HConnection connection = HConnectionManager.createConnection(conf);
//       * HTableInterface table = connection.getTable("mytable");
//       * table.get(...); ...
//       * table.close();
//       * connection.close();
//       * */
//      conn = HConnectionManager.createConnection(getConfiguration());
//    }
//
//    return conn;
//  }
//}
