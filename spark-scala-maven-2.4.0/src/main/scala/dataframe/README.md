# _DataFrame_ Examples

`DataFrame` was introduced in Spark 1.3.0 to replace and generalize `SchemaRDD`. 
There is one significant difference between the two classes: while `SchemaRDD` extended `RDD[Row]`, `DataFrame` contains one. 
In most cases, where your code used a `SchemaRDD` in the past, it can now use a `DataFrame` without additional changes. 
However, the domain-specific language (DSL) for querying through a `SchemaRDD` without writing SQL has been strengthened considerably in the `DataFrame` API. 

Two important additional classes to understand are `Row` and `Column`.

This directory contains examples of using `DataFrame`, focusing on the aspects that are not strictly related to Spark SQL queries --
the specifically SQL oriented aspects are covered in the **sql** directory, which is a peer of this one. 

## Getting started

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| Basic.scala           | How to create a `DataFrame` from case classes, examine it and perform basic operations. **Start here.** |
| SimpleCreation.scala | Create essentially the same `DataFrame` as in Basic.scala from an `RDD` of tuples, and explicit column names. |
| FromRowsAndSchema.scala | Create essentially the same `DataFrame` as in Basic.scala from an `RDD[Row]` and a schema. |

## Querying

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| GroupingAndAggrgegation.scala | Several different ways to specify aggregation. |
| Select.scala          | How to extract data from a `DataFrame`. This is a good place to see how convenient the API can be. |
| DateTime.scala        | Functions for querying against DateType and DatetimeType. |

## Utilities

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| DropColumns.scala     | Creating a new DataFrame that omits some of the original DataFrame's columns. |
| DropDuplicates        | Crate a DataFrame that removes duplicate rows, or optionally removes rows that are only identical on certain specified columns. |
| Range.scala           | Using range() methods on SQLContext to create simple DataFrames with values from that range. |

## Advanced

| File                  | What's Illustrated    |
|-----------------------|-----------------------|
| ComplexSchema.scala   | Creating a DataFrame with various forms of complex schema -- start with FromRowsAndSchema.scala for a simpler example |
| Transform.scala       | How to transform one `DataFrame` to another: written in response to a [question on StackOverflow](http://stackoverflow.com/questions/29151348/operation-on-data-frame/29159604). |
| UDF.scala             | How to use user-defined functions (UDFs) in queries. Note that the use of UDFs in SQL queries is covered seperately in the **sql** directory. |
| UDT.scala             | User defined types from a DataFrame perspective -- depends on understanding UDF.scala |
| UDAF.scala            | A simple User Defined Aggregation Function as introduced in Spark 1.5.0 |
| DatasetConversion.scala | Explore interoperability between DataFrame and Dataset  -- note that Dataset is convered more detail in [dataset](../dataset/README.md) |

##Example


scala> spark.version
res0: String = 2.2.0.cloudera1

scala> val df = spark.createDataset(Seq(("key1", 23, 1.0), ("key1", 10, 2.0))).toDF("id", "rsrp", "rsrq")
df: org.apache.spark.sql.DataFrame = [id: string, rsrp: int ... 1 more field]


scala> df.show

+----+----+----+
|  id|rsrp|rsrq|
+----+----+----+
|key1|  23| 1.0|
|key1|  10| 2.0|
+----+----+----+


scala> df.printSchema

root
 |-- id: string (nullable = true)
 |-- rsrp: integer (nullable = false)
 |-- rsrq: double (nullable = false)
复制代码
第一种方式：使用lit()增加常量（固定值）
可以是字符串类型，整型

复制代码
scala> df.withColumn("sinurl", lit(12)).show 

+----+----+----+------+
|  id|rsrp|rsrq|sinurl|
+----+----+----+------+
|key1|  23| 1.0|    12|
|key1|  10| 2.0|    12|
+----+----+----+------+

scala> df.withColumn("type", lit("mr")).show 

+----+----+----+----+
|  id|rsrp|rsrq|type|
+----+----+----+----+
|key1|  23| 1.0|  mr|
|key1|  10| 2.0|  mr|
+----+----+----+----+
复制代码
注意：

lit()是spark自带的函数，需要import org.apache.spark.sql.functions

Since 1.3.0
def lit(literal: Any): Column Creates a Column of literal value. The passed in object is returned directly if it is already a Column. If the object is a Scala Symbol, it is converted into a Column also. Otherwise, a new Column is created to represent the literal value.

第二种方式：使用当前已有的某列的变换新增
复制代码
scala> df.withColumn("rsrp2", $"rsrp"*2).show 

+----+----+----+-----+
|  id|rsrp|rsrq|rsrp2|
+----+----+----+-----+
|key1|  23| 1.0|   46|
|key1|  10| 2.0|   20|
+----+----+----+-----+
复制代码
第三种方式：使用select函数增加列
复制代码
scala> import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataTypes
scala> df.select(col("*"), 
     |     udf{
     |         (e:Int) =>
     |             if(e == "23") {
     |                 1
     |             } else {
     |                 2
     |             }
     |     }.apply(df("rsrp")).cast(DataTypes.DoubleType).as("rsrp_udf")
     | ).show
+----+----+----+--------+
|  id|rsrp|rsrq|rsrp_udf|
+----+----+----+--------+
|key1|  23| 1.0|     2.0|
|key1|  10| 2.0|     2.0|
+----+----+----+--------+
复制代码
复制代码
scala> df.select(col("*"),
     |     when(df("rsrp") > 10, lit(">10")).when(df("rsrp") === 10, "=10").otherwise("<10").as("rsrp_compare10")
     | ).show
+----+----+----+--------------+
|  id|rsrp|rsrq|rsrp_compare10|
+----+----+----+--------------+
|key1|  23| 1.0|           >10|
|key1|  10| 2.0|           =10|
+----+----+----+--------------+
复制代码
第四种方式：case when当参数嵌套udf
df.withColumn("r",
   when($"rsrp".isNull, lit(null))
       .otherwise(udf1($"rsrp"))
       .cast(DataTypes.IntegerType)
)
第五种方式：使用expr()函数
复制代码
scala> df.withColumn("rsrp4", expr("rsrp * 4")).show
+----+----+----+-----+
|  id|rsrp|rsrq|rsrp4|
+----+----+----+-----+
|key1|  23| 1.0|   92|
|key1|  10| 2.0|   40|
+----+----+----+-----+
复制代码
Dataset删除列
复制代码
scala> df.drop("rsrp").show
+----+----+
|  id|rsrq|
+----+----+
|key1| 1.0|
|key1| 2.0|
+----+----+


scala> df.drop("rsrp","rsrq").show
+----+
|  id|
+----+
|key1|
|key1|
+----+
复制代码
Dataset替换null列
首先，在hadoop目录/user/spark/test.csv

复制代码
[spark@master ~]$ hadoop fs -text /user/spark/test.csv
key1,key2,key3,key4,key5
aaa,1,2,t1,4
bbb,5,3,t2,8
ccc,2,2,,7
,7,3,t1,
bbb,1,5,t3,0
,4,,t1,8 
复制代码
备注：如果想在根目录下执行spark-shell.需要在/etc/profile中追加spark的安装目录：

export SPARK_HOME=/opt/spark-2.2.1-bin-hadoop2.7
export PATH=$PATH:$SPARK_HOME/bin
使用spark加载.user/spark/test.csv文件

复制代码
[spark@master ~]$ spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
18/10/29 21:50:32 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://192.168.0.120:4040
Spark context available as 'sc' (master = local[*], app id = local-1540821032565).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.2.1
      /_/
         
Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_171)
Type in expressions to have them evaluated.
Type :help for more information.

scala> val df = spark.read.option("header","true").csv("/user/spark/test.csv")
18/10/29 21:51:16 WARN metastore.ObjectStore: Version information not found in metastore. hive.metastore.schema.verification is not enabled so recording the schema version 1.2.0
18/10/29 21:51:16 WARN metastore.ObjectStore: Failed to get database default, returning NoSuchObjectException
18/10/29 21:51:37 WARN metastore.ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
df: org.apache.spark.sql.DataFrame = [key1: string, key2: string ... 3 more fields]

scala> df.show
+----+----+----+----+----+
|key1|key2|key3|key4|key5|
+----+----+----+----+----+
| aaa|   1|   2|  t1|   4|
| bbb|   5|   3|  t2|   8|
| ccc|   2|   2|null|   7|
|null|   7|   3|  t1|null|
| bbb|   1|   5|  t3|   0|
|null|   4|null|  t1|  8 |
+----+----+----+----+----+

scala> df.schema
res3: org.apache.spark.sql.types.StructType = StructType(StructField(key1,StringType,true), StructField(key2,StringType,true), 
StructField(key3,StringType,true), StructField(key4,StringType,true), StructField(key5,StringType,true))

scala> df.printSchema
root
 |-- key1: string (nullable = true)
 |-- key2: string (nullable = true)
 |-- key3: string (nullable = true)
 |-- key4: string (nullable = true)
 |-- key5: string (nullable = true)
复制代码
一次修改相同类型的多个列的示例。 这里是把key3,key5列中所有的null值替换成1024。 csv导入时默认是string，如果是整型，写法是一样的，有各个类型的重载。

复制代码
scala>  df.na.fill("1024",Seq("key3","key5")).show
+----+----+----+----+----+
|key1|key2|key3|key4|key5|
+----+----+----+----+----+
| aaa|   1|   2|  t1|   4|
| bbb|   5|   3|  t2|   8|
| ccc|   2|   2|null|   7|
|null|   7|   3|  t1|1024|
| bbb|   1|   5|  t3|   0|
|null|   4|1024|  t1|  8 |
+----+----+----+----+----+
复制代码
一次修改不同类型的多个列的示例。 csv导入时默认是string，如果是整型，写法是一样的，有各个类型的重载。

复制代码
scala> df.na.fill(Map(("key1"->"yyy"),("key3","1024"),("key4","t88"),("key5","4096"))).show
+----+----+----+----+----+
|key1|key2|key3|key4|key5|
+----+----+----+----+----+
| aaa|   1|   2|  t1|   4|
| bbb|   5|   3|  t2|   8|
| ccc|   2|   2| t88|   7|
| yyy|   7|   3|  t1|4096|
| bbb|   1|   5|  t3|   0|
| yyy|   4|1024|  t1|  8 |
+----+----+----+----+----+
复制代码
不修改，只是过滤掉含有null值的行。 这里是过滤掉key3,key5列中含有null的行

复制代码
scala>  df.na.drop(Seq("key3","key5")).show
+----+----+----+----+----+
|key1|key2|key3|key4|key5|
+----+----+----+----+----+
| aaa|   1|   2|  t1|   4|
| bbb|   5|   3|  t2|   8|
| ccc|   2|   2|null|   7|
| bbb|   1|   5|  t3|   0|
+----+----+----+----+----+
复制代码
过滤掉指定的若干列中，有效值少于n列的行 这里是过滤掉key1,key2,key3这3列中有效值小于2列的行。最后一行中，这3列有2列都是null，所以被过滤掉了。

复制代码
scala> df.na.drop(2,Seq("key1","key2","key3")).show
+----+----+----+----+----+
|key1|key2|key3|key4|key5|
+----+----+----+----+----+
| aaa|   1|   2|  t1|   4|
| bbb|   5|   3|  t2|   8|
| ccc|   2|   2|null|   7|
|null|   7|   3|  t1|null|
| bbb|   1|   5|  t3|   0|
+----+----+----+----+----+
复制代码
同上，如果不指定列名列表，则默认列名列表就是所有列

复制代码
scala> df.na.drop(4).show
+----+----+----+----+----+
|key1|key2|key3|key4|key5|
+----+----+----+----+----+
| aaa|   1|   2|  t1|   4|
| bbb|   5|   3|  t2|   8|
| ccc|   2|   2|null|   7|
| bbb|   1|   5|  t3|   0|
+----+----+----+----+----+