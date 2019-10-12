/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.sql

import org.apache.spark.sql.{Row, SaveMode}
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$

// One method for defining the schema of an RDD is to make a case class with the desired column
// names and types.
case class Record(key: Int, value: String)

object RDDRelation {
  def main(args: Array[String]) {
    // $example on:init_session$
    val spark = SparkSession
      .builder
      .appName("Spark Examples")
      .config("spark.some.config.option", "some-value").master("local")
      .getOrCreate()


    // Importing the SparkSession gives access to all the SQL functions and implicit conversions.
    import spark.implicits._
    // $example off:init_session$

    val df = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    // Any RDD containing case classes can be used to create a temporary view.  The schema of the
    // view is automatically inferred using scala reflection.
    df.show(20)
    df.createOrReplaceTempView("records")

    // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")
    spark.sql("SELECT * FROM records").show(20)//.collect().foreach(println)

    // Aggregation queries are also supported.
    val count = spark.sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    // The results of SQL queries are themselves RDDs and support all normal RDD functions. The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    val rddFromSql = spark.sql("SELECT key, value FROM records WHERE key < 10")

    println("Result of RDD.map:")
    rddFromSql.rdd.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)

    // Queries can also be written using a LINQ-like Scala DSL.
    df.where($"key" === 1).orderBy($"value".asc).select($"key").collect().foreach(println)

    // Write out an RDD as a parquet file with overwrite mode.
    df.write.mode(SaveMode.Overwrite).parquet("spark-warehouse/test.parquet")
    // df.write.mode(SaveMode.Overwrite).parquet("spark-warehouse")
    // Read in parquet file.  Parquet files are self-describing so the schema is preserved.
    var parquetFile = spark.read.parquet("spark-warehouse/test.parquet")
    import org.apache.spark.sql.functions._
    parquetFile=parquetFile.withColumn("hua",concat_ws("," ,$"key" , $"value"))
    //方法3：使用自定义UDF函数

    // 编写udf函数
    def mergeCols(row: Row): String = {
      row.toSeq.foldLeft("")(_ + "," + _).substring(1)
    }
    val mergeColsUDF = udf(mergeCols _)

    parquetFile.select(mergeColsUDF(struct($"value", $"hua")).as("value2")).show()

    println(parquetFile.printSchema())
    //df.select(concat_ws(separator, $"name", $"age", $"phone").cast(StringType).as("value")).show()
    // Queries can be run using the DSL on parquet files just like the original RDD.
    parquetFile.where("key==1").show(20)

    parquetFile.where($"key" === 1).select($"value".as("a")).show(20)//.collect().foreach(println)

    // These files can also be used to create a temporary view.
    parquetFile.createOrReplaceTempView("parquetFile")
    spark.sql("SELECT * FROM parquetFile").collect().foreach(println)

    spark.stop()
  }
}
// scalastyle:on println
/*
dataframe的基本操作
1、 cache()同步数据的内存

2、 columns 返回一个string类型的数组，返回值是所有列的名字

3、 dtypes返回一个string类型的二维数组，返回值是所有列的名字以及类型

4、 explan()打印执行计划  物理的

5、 explain(n:Boolean) 输入值为 false 或者true ，返回值是unit  默认是false ，如果输入true 将会打印 逻辑的和物理的

6、 isLocal 返回值是Boolean类型，如果允许模式是local返回true 否则返回false

7、 persist(newlevel:StorageLevel) 返回一个dataframe.this.type 输入存储模型类型

8、 printSchema() 打印出字段名称和类型 按照树状结构来打印

9、 registerTempTable(tablename:String) 返回Unit ，将df的对象只放在一张表里面，这个表随着对象的删除而删除了

10、 schema 返回structType 类型，将字段名称和类型按照结构体类型返回

11、 toDF()返回一个新的dataframe类型的

12、 toDF(colnames：String*)将参数中的几个字段返回一个新的dataframe类型的，

13、 unpersist() 返回dataframe.this.type 类型，去除模式中的数据

14、 unpersist(blocking:Boolean)返回dataframe.this.type类型 true 和unpersist是一样的作用false 是去除RDD

集成查询：
1、 agg(expers:column*) 返回dataframe类型 ，同数学计算求值

df.agg(max("age"), avg("salary"))

df.groupBy().agg(max("age"), avg("salary"))

2、 agg(exprs: Map[String, String])  返回dataframe类型 ，同数学计算求值 map类型的

df.agg(Map("age" -> "max", "salary" -> "avg"))

df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))

3、 agg(aggExpr: (String, String), aggExprs: (String, String)*)  返回dataframe类型 ，同数学计算求值

df.agg(Map("age" -> "max", "salary" -> "avg"))

df.groupBy().agg(Map("age" -> "max", "salary" -> "avg"))

4、 apply(colName: String) 返回column类型，捕获输入进去列的对象

5、 as(alias: String) 返回一个新的dataframe类型，就是原来的一个别名

6、 col(colName: String)  返回column类型，捕获输入进去列的对象

7、 cube(col1: String, cols: String*) 返回一个GroupedData类型，根据某些字段来汇总

8、 distinct 去重 返回一个dataframe类型

9、 drop(col: Column) 删除某列 返回dataframe类型

10、 dropDuplicates(colNames: Array[String]) 删除相同的列 返回一个dataframe

11、 except(other: DataFrame) 返回一个dataframe，返回在当前集合存在的在其他集合不存在的

12、 explode[A, B](inputColumn: String, outputColumn: String)(f: (A) ⇒ TraversableOnce[B])(implicit arg0: scala.reflect.api.JavaUniverse.TypeTag[B]) 返回值是dataframe类型，这个 将一个字段进行更多行的拆分

df.explode("name","names") {name :String=> name.split(" ")}.show();

将name字段根据空格来拆分，拆分的字段放在names里面

13、 filter(conditionExpr: String): 刷选部分数据，返回dataframe类型 df.filter("age>10").show();  df.filter(df("age")>10).show();   df.where(df("age")>10).show(); 都可以

14、 groupBy(col1: String, cols: String*) 根据某写字段来汇总返回groupedate类型   df.groupBy("age").agg(Map("age" ->"count")).show();df.groupBy("age").avg().show();都可以

15、 intersect(other: DataFrame) 返回一个dataframe，在2个dataframe都存在的元素

16、 join(right: DataFrame, joinExprs: Column, joinType: String)

一个是关联的dataframe，第二个关联的条件，第三个关联的类型：inner, outer, left_outer, right_outer, leftsemi

df.join(ds,df("name")===ds("name") and  df("age")===ds("age"),"outer").show();

17、 limit(n: Int) 返回dataframe类型  去n 条数据出来

18、 na: DataFrameNaFunctions ，可以调用dataframenafunctions的功能区做过滤 df.na.drop().show(); 删除为空的行

19、 orderBy(sortExprs: Column*) 做alise排序

20、 select(cols:string*) dataframe 做字段的刷选 df.select($"colA", $"colB" + 1)

21、 selectExpr(exprs: String*) 做字段的刷选 df.selectExpr("name","name as names","upper(name)","age+1").show();

22、 sort(sortExprs: Column*) 排序 df.sort(df("age").desc).show(); 默认是asc

23、 unionAll(other:Dataframe) 合并 df.unionAll(ds).show();

24、 withColumnRenamed(existingName: String, newName: String) 修改列表 df.withColumnRenamed("name","names").show();

25、 withColumn(colName: String, col: Column) 增加一列 df.withColumn("aa",df("name")).show();


 */