package sljr.ybd.spark.test.hbase
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName,HColumnDescriptor}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
object hbase_con_old {
  def createHTable(connection: Connection,tablename: String): Unit= {
    //Hbase表模式管理器
    val admin = connection.getAdmin
    //本例将操作的表名
    val tableName = TableName.valueOf(tablename)
    //如果需要创建表
    if (!admin.tableExists(tableName)) {
      //创建Hbase表模式
      val tableDescriptor = new HTableDescriptor(tableName)
      //创建列簇1    artitle
      tableDescriptor.addFamily(new HColumnDescriptor("artitle".getBytes()))
      //创建列簇2    author
      tableDescriptor.addFamily(new HColumnDescriptor("author".getBytes()))
      //创建表
      admin.createTable(tableDescriptor)
      println("create done.")
    }
  }
  //删除表
  def deleteHTable(connection:Connection,tablename:String):Unit={
    //本例将操作的表名
    val tableName = TableName.valueOf(tablename)
    //Hbase表模式管理器
    val admin = connection.getAdmin
    if (admin.tableExists(tableName)){
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }
  }
  //插入记录
  def insertHTable(connection:Connection,tablename:String,family:String,column:String,key:String,value:String):Unit={
    try{
      val userTable = TableName.valueOf(tablename)
      val table=connection.getTable(userTable)
      //准备key 的数据
      val p=new Put(key.getBytes)
      //为put操作指定 column 和 value
      p.addColumn(family.getBytes,column.getBytes,value.getBytes())
      //验证可以提交两个clomun？？？？不可以
      // p.addColumn(family.getBytes(),"china".getBytes(),"JAVA for china".getBytes())
      //提交一行
      table.put(p)
    }
  }
  //基于KEY查询某条数据
  def getAResult(connection:Connection,tablename:String,family:String,column:String,key:String):Unit={
    var table:Table=null
    try{
      val userTable = TableName.valueOf(tablename)
      table=connection.getTable(userTable)
      val g=new Get(key.getBytes())
      val result=table.get(g)
      val value=Bytes.toString(result.getValue(family.getBytes(),column.getBytes()))
      println("key:"+value)
    }finally{
      if(table!=null)table.close()

    }

  }

  //删除某条记录
  def deleteRecord(connection:Connection,tablename:String,family:String,column:String,key:String): Unit ={
    var table:Table=null
    try{
      val userTable=TableName.valueOf(tablename)
      table=connection.getTable(userTable)
      val d=new Delete(key.getBytes())
      d.addColumn(family.getBytes(),column.getBytes())
      table.delete(d)
      println("delete record done.")
    }finally{
      if(table!=null)table.close()
    }
  }

  //扫描记录
  def scanRecord(connection:Connection,tablename:String,family:String,column:String): Unit ={
    var table:Table=null
    var scanner:ResultScanner=null
    try{
      val userTable=TableName.valueOf(tablename)
      table=connection.getTable(userTable)
      val s=new Scan()
      s.addColumn(family.getBytes(),column.getBytes())
      scanner=table.getScanner(s)
      println("scan...for...")
      var result:Result=scanner.next()
      while(result!=null){
        println("Found row:" + result)
        println("Found value: "+Bytes.toString(result.getValue(family.getBytes(),column.getBytes())))
        result=scanner.next()
      }


    }finally{
      if(table!=null)
        table.close()
      scanner.close()
    }
  }
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("HBaseTest")
    val sc = new SparkContext(sparkConf)
    var table_name = "yfgtest:huatest"
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","cdh1,cdh2,cdh3");
    conf.set(TableInputFormat.INPUT_TABLE, table_name)
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    val connection = ConnectionFactory.createConnection(conf);
    val admin = connection.getAdmin()
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    println(hBaseRDD.count())
    val listtable=admin.listTableNames()
    //admin.listTableNames().foreach(a => println(a))
    println(TableName.valueOf("yfgtest:huadds"))
    try {
      createHTable(connection, "blog")
      //插入数据,重复执行为覆盖
      insertHTable(connection,"blog","artitle","engish","002","c++ for me")
      insertHTable(connection,"blog","artitle","engish","003","python for me")
      insertHTable(connection,"blog","artitle","chinese","002","C++ for china")
      //删除记录
      // deleteRecord(connection,"blog","artitle","chinese","002")
      //扫描整个表
      scanRecord(connection,"blog","artitle","engish")
      //删除表测试
      // deleteHTable(connection, "blog")
    }finally {
      connection.close
      //   sc.stop
    }
    //insert_hbase(100002,3)
  }

}