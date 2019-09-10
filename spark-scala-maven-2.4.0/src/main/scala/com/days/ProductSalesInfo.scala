package com.days

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.lang.Double

//����case class����Schema
//��Ʒ��
case class Product(prod_id:Int,prod_name:String)
//������
case class SaleOrder(prod_id:Int,year_id:Int,amount:Double)

object ProductSalesInfo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ProductSalesInfo").setMaster("local")
    
    //����SparkSession��Ҳ����ֱ�Ӵ���SQLContext
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._    
    
    //��һ����ȡ����Ʒ������
    val productInfo = sc.textFile("hdfs://hdp21:8020/input/sh/products").map(line=>{
      val words = line.split(",")
      //���أ� ��Ʒ��ID����Ʒ������
      (Integer.parseInt(words(0)),words(1))
    }).map(d=>Product(d._1,d._2)).toDF()
        
    //�ڶ�����ȡ������������
    val orderInfo = sc.textFile("hdfs://hdp21:8020/input/sh/sales").map(line=>{
      val words = line.split(",")
      
      //���أ� ��Ʒ��ID����ݡ����
      (Integer.parseInt(words(0)),
       Integer.parseInt(words(2).substring(0, 4)),    //1998-01-10
       Double.parseDouble(words(6))
       )
    }).map(d=>SaleOrder(d._1,d._2,d._3)).toDF()
    
    //ע����ͼ
    productInfo.createTempView("product")
    orderInfo.createTempView("saleorder")
    
    //��ѯ
    /*
	    select prod_name,year_id,sum(amount)
      from product,saleorder
      where product.prod_id=saleorder.prod_id
      group by prod_name,year_id
     */
    //��һ�����õ����
    var sql1 = "select prod_name,year_id,sum(amount) "
    sql1 = sql1 + " from product,saleorder "
    sql1 = sql1 + " where product.prod_id=saleorder.prod_id"
    sql1 = sql1 + " group by prod_name,year_id"
    
    val result = sqlContext.sql(sql1).toDF("prod_name","year_id","total")
    result.createTempView("result")
    
    //�ڶ���������ת����
    sqlContext.sql("select prod_name,sum(case year_id when 1998 then total else 0 end),sum(case year_id when 1999 then total else 0 end),sum(case year_id when 2000 then total else 0 end),sum(case year_id when 2001 then total else 0 end) from result group by prod_name").show
    
    sc.stop()
  }
}





















