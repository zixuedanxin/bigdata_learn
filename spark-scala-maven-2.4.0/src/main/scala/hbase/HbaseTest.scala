package hbase
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.junit.{Assert, Test}

import scala.collection.mutable.ArrayBuffer

class HbaseTest {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "localhost")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  val hBaseUtils = new HbaseUtils()
  val admin = hBaseUtils.getAdmin(conf)

  /**
    * 创建表
    */
  @Test
  def createTable(): Unit = {
        val list = List("family1", "family2")
   // val list = List("family1")
    hBaseUtils.createTable(admin, "test3", list)
  }

  /**
    * 插入数据
    */
  @Test
  def insertData(): Unit = {
    hBaseUtils.insertData(conf, "test2", "rowkey1", "family1", "李四", "lisi2")
  }

  /**
    * 批量插入数据
    */
  @Test
  def batchInsertData: Unit = {
    hBaseUtils.batchInsertData(conf, "test2", "rowkey2", "family2", "name", "lisi")
  }

  /**
    * 获取指定的一行
    */
  @Test
  def getRow: Unit = {
    val row: Array[Cell] = hBaseUtils.getRow(conf, "test2", "rowkey2")
    row.foreach(a => {
      println(new String(a.getRow()) + " " + a.getTimestamp + " " + new String(a.getFamily()) + " " + new String(a.getValue))
    })
  }

  /**
    * 删除指定的一行
    */
  @Test
  def delRow: Unit = {
    hBaseUtils.delRow(conf, "test2", "rowkey1")
  }

  /**
    * 扫描全表
    */
  @Test
  def getByScan: Unit = {
    val all: ArrayBuffer[Array[Cell]] = hBaseUtils.getByScan(conf, "test2")
    all.foreach(arrBuffer => arrBuffer.foreach(cell => {
      println(new String(cell.getRowArray, cell.getRowOffset, cell.getRowLength) + "-->Row")
      println(cell.getTimestamp + "-->timpsstamp  ")
      println(new String(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength) + "-->family  ")
      println(new String(cell.getValueArray, cell.getValueOffset, cell.getValueLength) + "-->value  ")
      println(new String(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength) + " -->Tags")
    }))
  }
}