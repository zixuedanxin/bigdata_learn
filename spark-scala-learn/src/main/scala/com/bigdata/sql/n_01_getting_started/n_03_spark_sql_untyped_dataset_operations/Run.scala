package com.bigdata.sql.n_01_getting_started.n_03_spark_sql_untyped_dataset_operations

import com.bigdata.standalone.base.BaseSparkSession

object Run extends BaseSparkSession{

  def main(args: Array[String]): Unit = {

    val spark = sparkSession(isLocal = true)

    import spark.implicits._
    //val df = spark.read.json("file:///opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/src/main/resource/people.json")
    //返回dataFrame
    val df = spark.read.json("file:///"+ getProjectPath +"/src/main/resource/data/json/people.json")
    //打应有哪些列(类型)
    df.printSchema()
    //显示name列的数据
    df.select("name").show()
    //显示name列,age列的数据
    import org.apache.spark.sql.functions.{lit,coalesce}
    df.select("name", "age").show()
    df.select($"name", (coalesce($"age",lit(0))+10).as("age+10")).show()
    //显示name列,age列的数据,支持对列的表达式处理,进行计算
    df.selectExpr("name", "coalesce(age,0) +1" ).show()
    println("======filter")
    df.filter("age >21").show
    df.filter($"age" > 21).show
    println("======where")
    df.where("age >21").show
    df.where($"age" >21).show
    println("======groupBy")
    df.groupBy("age").count().show()
    df.groupBy("age").agg(Map("name" -> "count"))
    println("======show")
    df.show()
    spark.stop()
  }

}

/**
  * 1、agg(expers:column*) 返回dataframe类型 ，同数学计算求值
  * df.agg(max("age"), avg("salary"))
  * df.groupBy().agg(max("age"), avg("salary"))
  * 2、 agg(exprs: Map[String, String])  返回dataframe类型 ，同数学计算求值 map类型的
  * df.agg(Map("age" -> "max", "salary" -> "avg"))
  * df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
  * 3、 agg(aggExpr: (String, String), aggExprs: (String, String)*)  返回dataframe类型 ，同数学计算求值
  * df.agg(Map("age" -> "max", "salary" -> "avg"))
  * df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))
  * 例子1：
  * scala> spark.version
  * res2: String = 2.0.2
  *
  * scala> case class Test(bf: Int, df: Int, duration: Int, tel_date: Int)
  * defined class Test
  *
  * scala> val df = Seq(Test(1,1,1,1), Test(1,1,2,2), Test(1,1,3,3), Test(2,2,3,3), Test(2,2,2,2), Test(2,2,1,1)).toDF
  * df: org.apache.spark.sql.DataFrame = [bf: int, df: int ... 2 more fields]
  *
  * scala> df.show
  * +---+---+--------+--------+
  * | bf| df|duration|tel_date|
  * +---+---+--------+--------+
  * |  1|  1|       1|       1|
  * |  1|  1|       2|       2|
  * |  1|  1|       3|       3|
  * |  2|  2|       3|       3|
  * |  2|  2|       2|       2|
  * |  2|  2|       1|       1|
  * +---+---+--------+--------+
  *
  *
  * scala> df.groupBy("bf", "df").agg(("duration","sum"),("tel_date","min"),("tel_date","max")).show()
  * +---+---+-------------+-------------+-------------+
  * | bf| df|sum(duration)|min(tel_date)|max(tel_date)|
  * +---+---+-------------+-------------+-------------+
  * |  2|  2|            6|            1|            3|
  * |  1|  1|            6|            1|            3|
  *
  * +---+---+-------------+-------------+-------------+
  * 注意：此处df已经少了列duration和tel_date，只有groupby的key和agg中的字段
  *
  * 例子2：
  * import pyspark.sql.functions as func
  * agg(func.max("event_time").alias("max_event_tm"),func.min("event_time").alias("min_event_tm"))
  */
