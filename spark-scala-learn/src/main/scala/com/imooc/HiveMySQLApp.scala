package com.imooc

import org.apache.spark.sql.SparkSession

/**
 * 使用外部数据源综合查询Hive和MySQL的表数据
  *  外部数据源综合案例
create database spark;
use spark;

CREATE TABLE DEPT(
DEPTNO int(2) PRIMARY KEY,
DNAME VARCHAR(14) ,
LOC VARCHAR(13) ) ;

INSERT INTO DEPT VALUES(10,'ACCOUNTING','NEW YORK');
INSERT INTO DEPT VALUES(20,'RESEARCH','DALLAS');
INSERT INTO DEPT VALUES(30,'SALES','CHICAGO');
INSERT INTO DEPT VALUES(40,'OPERATIONS','BOSTON');

  */
object HiveMySQLApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("HiveMySQLApp")
      .master("local[2]").getOrCreate()

    // 加载Hive表数据
    val hiveDF = spark.table("emp")

    // 加载MySQL表数据
    val mysqlDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306").option("dbtable", "spark.DEPT").option("user", "root").option("password", "root").option("driver", "com.mysql.jdbc.Driver").load()

    // JOIN
    val resultDF = hiveDF.join(mysqlDF, hiveDF.col("deptno") === mysqlDF.col("DEPTNO"))
    resultDF.show


    resultDF.select(hiveDF.col("empno"),hiveDF.col("ename"),
      mysqlDF.col("deptno"), mysqlDF.col("dname")).show

    spark.stop()
  }

}
