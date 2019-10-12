package dataset

import org.apache.spark.sql.SparkSession

//
// Create Datasets of primitive type and tuple type ands show simple operations.
//
object Basic {

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("Dataset-Basic")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    // Create a tiny Dataset of integers
    val s = Seq(10, 11, 12, 13, 14, 15)
    val ds = s.toDS()

    println("*** only one column, and it always has the same name")
    ds.columns.foreach(println(_))

    println("*** column types")
    ds.dtypes.foreach(println(_))

    println("*** schema as if it was a DataFrame")
    ds.printSchema()

    println("*** values > 12")
    ds.where($"value" > 12).show()

    // This seems to be the best way to get a range that's actually a Seq and
    // thus easy to convert to a Dataset, rather than a Range, which isn't.
    val s2 = Seq.range(1, 100)

    println("*** size of the range")
    println(s2.size)

    val tuples = Seq((1, "one", "un"), (2, "two", "deux"), (3, "three", "trois"))
    val tupleDS = tuples.toDS()
    println("*** only one column, and it always has the same name")
    tupleDS.columns.foreach(println(_))
    println("*** Tuple Dataset types")
    tupleDS.dtypes.foreach(println(_))

    // the tuple columns have unfriendly names, but you can use them to query
    println("*** filter by one column and fetch another")
    tupleDS.where($"_1" > 2).select($"_2", $"_3").show()

  }
}

/*
https://www.cnblogs.com/lestatzhang/p/10611320.html
RDD、DataFrame和DataSet的定义
在开始Spark RDD与DataFrame与Dataset之间的比较之前，先让我们看一下Spark中的RDD，DataFrame和Datasets的定义：

Spark RDD
RDD代表弹性分布式数据集。它是记录的只读分区集合。 RDD是Spark的基本数据结构。它允许程序员以容错方式在大型集群上执行内存计算。
Spark Dataframe
与RDD不同，数据组以列的形式组织起来，类似于关系数据库中的表。它是一个不可变的分布式数据集合。
Spark中的DataFrame允许开发人员将数据结构(类型)加到分布式数据集合上，从而实现更高级别的抽象。
Spark Dataset
Apache Spark中的Dataset是DataFrame API的扩展，它提供了类型安全(type-safe)，面向对象(object-oriented)的编程接口。 Dataset利用Catalyst optimizer可以让用户通过类似于sql的表达式对数据进行查询。
RDD、DataFrame和DataSet的比较
Spark版本
RDD – 自Spark 1.0起
DataFrames – 自Spark 1.3起
DataSet – 自Spark 1.6起
数据表示形式
RDD
RDD是分布在集群中许多机器上的数据元素的分布式集合。 RDD是一组表示数据的Java或Scala对象。
DataFrame
DataFrame是命名列构成的分布式数据集合。 它在概念上类似于关系数据库中的表。
Dataset
它是DataFrame API的扩展，提供RDD API的类型安全，面向对象的编程接口以及Catalyst查询优化器的性能优势和DataFrame API的堆外存储机制的功能。
数据格式
RDD
它可以轻松有效地处理结构化和非结构化的数据。 和Dataframe和DataSet一样，RDD不会推断出所获取的数据的结构类型，需要用户来指定它。
DataFrame
仅适用于结构化和半结构化数据。 它的数据以命名列的形式组织起来。
DataSet
它也可以有效地处理结构化和非结构化数据。 它表示行(row)的JVM对象或行对象集合形式的数据。 它通过编码器以表格形式(tabular forms)表示。
编译时类型安全
RDD
RDD提供了一种熟悉的面向对象编程风格，具有编译时类型安全性。
DataFrame
如果您尝试访问表中不存在的列，则持编译错误。 它仅在运行时检测属性错误。
DataSet
DataSet可以在编译时检查类型, 它提供编译时类型安全性。
[TO-DO 什么是编译时的类型安全]
序列化
RDD
每当Spark需要在集群内分发数据或将数据写入磁盘时，它就会使用Java序列化。序列化单个Java和Scala对象的开销很昂贵，并且需要在节点之间发送数据和结构。

DataFrame
Spark DataFrame可以将数据序列化为二进制格式的堆外存储（在内存中），然后直接在此堆内存上执行许多转换。无需使用java序列化来编码数据。它提供了一个Tungsten物理执行后端，来管理内存并动态生成字节码以进行表达式评估。

DataSet
在序列化数据时，Spark中的数据集API具有编码器的概念，该编码器处理JVM对象与表格表示之间的转换。它使用spark内部Tungsten二进制格式存储表格表示。数据集允许对序列化数据执行操作并改善内存使用。它允许按需访问单个属性，而不会消灭整个对象。

垃圾回收
RDD
创建和销毁单个对象会导致垃圾回收。
DataFrame
避免在为数据集中的每一行构造单个对象时引起的垃圾回收。
DataSet
因为序列化是通过Tungsten进行的，它使用了off heap数据序列化，不需要垃圾回收器来摧毁对象
效率/内存使用
RDD
在java和scala对象上单独执行序列化时，效率会降低，这需要花费大量时间。
DataFrame
使用off heap内存进行序列化可以减少开销。 它动态生成字节代码，以便可以对该序列化数据执行许多操作。 无需对小型操作进行反序列化。
DataSet
它允许对序列化数据执行操作并改善内存使用。 因此，它可以允许按需访问单个属性，而无需反序列化整个对象。
编程语言支持
RDD
RDD提供Java，Scala，Python和R语言的API。 因此，此功能为开发人员提供了灵活性。
DataFrame
DataFrame同样也提供Java，Scala，Python和R语言的API
DataSet
Dataset 的一些API目前仅支持Scala和Java，对Python和R语言的API在陆续开发中
聚合操作(Aggregation)
RDD
RDD API执行简单的分组和聚合操作的速度较慢。
DataFrame
DataFrame API非常易于使用。 探索性分析更快，在大型数据集上创建汇总统计数据。
DataSet
在Dataset中，对大量数据集执行聚合操作的速度更快。
结论
当我们需要对数据集进行底层的转换和操作时， 可以选择使用RDD
当我们需要高级抽象时，可以使用DataFrame和Dataset API。
对于非结构化数据，例如媒体流或文本流，同样可以使用DataFrame和Dataset API。
我们可以使用DataFrame和Dataset 中的高级的方法。 例如，filter, maps, aggregation, sum, SQL queries以及通过列访问数据等
如果您不关心在按名称或列处理或访问数据属性时强加架构（例如列式格式）。
另外，如果我们想要在编译时更高程度的类型安全性。
 */