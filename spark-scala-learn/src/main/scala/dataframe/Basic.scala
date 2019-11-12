package dataframe

import org.apache.spark.sql.SparkSession

//https://www.cnblogs.com/code2one/p/9872010.html
// Create a DataFrame based on an RDD of case class objects and perform some basic
// DataFrame operations. The DataFrame can instead be created more directly from
// the standard building blocks -- an RDD[Row] and a schema -- see the example
// FromRowsAndSchema.scala to see how to do that.
//https://blog.csdn.net/qq_25948717/article/details/83114400
object Basic {
  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrame-Basic")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    // create a sequence of case class objects
    // (we defined the case class above)
    val custs = Seq(
      Cust(1, "Widget Co", 120000.00, 0.00, "AZ"),
      Cust(2, "Acme Widgets", 410500.00, 500.00, "CA"),
      Cust(3, "Widgetry", 410500.00, 200.00, "CA"),
      Cust(4, "Widgets R Us", 410500.00, 0.0, "CA"),
      Cust(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    // make it an RDD and convert to a DataFrame
    val customerDF = spark.sparkContext.parallelize(custs, 4).toDF()

    println("*** toString() just gives you the schema")

    println(customerDF.toString())

    println("*** It's better to use printSchema()")

    customerDF.printSchema()

    println("*** show() gives you neatly formatted data")

    customerDF.show()

    println("*** use select() to choose one column")

    customerDF.select("id").show()

    println("*** use select() for multiple columns")

    customerDF.select("sales", "state").show()

    println("*** use filter() to choose rows")

    customerDF.filter($"state".equalTo("CA")).show()
    customerDF.filter("state='CA'").show()


  }
}

/*
1. RDD转DataFrame
 

1. 构建schema

主要有三步：

构建RDD[Row]
构建schema
调用createDataFrame方法
object RddToDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RddToDataFrame").master("local").getOrCreate()

    val rdd = spark.sparkContext.textFile("file:///d:/data/words.txt")
        .persist(StorageLevel.MEMORY_ONLY)

    // 1. 构建RDD[Row],将一行的数据放入Row()中
    val rdd2 = rdd.flatMap(_.split(",")).map(t =>{Row(t._2,t._1)})

    // 2. 构建schema
    val schema = StructType{
      List(
        StructField("id",LongType,true),
        StructField("user",StringType,true)
      )}

    // 3. createDataFrame()
    val df = spark.createDataFrame(rdd2,schema)

    spark.stop()
  }
}
2. 自动推断 

将一行数据放入元组()中，toDF()中指定字段名，需要导入隐式转换。

object RddToDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RddToDataFrame").master("local").getOrCreate()

    val rdd = spark.sparkContext.textFile("file:///d:/data/words.txt")

    // 导入隐式转换
    import spark.implicits._

    val df = rdd.map{
      x => {
        val tmp =  x.split(",")
        (tmp(0).toInt, tmp(1))
      }
    }.toDF("id","name")


    spark.stop()
  }
}
3. 通过反射获取schema

跟自动推断差不多，不过需要创建一个case类，定义类属性。Spark通过反射将case类属性映射成Table表结构，字段名已经通过反射获取。需要导入隐式转换。

// case类
case class Words(id:Long,name:String) extends Serializable

object RddToDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RddToDataFrame").master("local").getOrCreate()

    val rdd = spark.sparkContext.textFile("file:///d:/data/words.txt")

    import spark.implicits._

    val df = rdd.map{
      x => {
        val tmp =  x.split(",")
        Words(tmp(0).toInt, tmp(1))
      }
    }.toDF()

    spark.stop()
  }
}
 
2. RDD转DataSet
跟转DataFrame差不多。

spark2.x以后，ScalaAPI中DataFrame只是Dataset[Row]类型的别名，所以转Dataset不用指定Row类型。

1. createDataset

object RddToDataset {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RddToDataset").master("local").getOrCreate()

    val rdd = spark.sparkContext.textFile("file:///d:/data/words.txt")

    import spark.implicits._

    val rdd2 = rdd.map(_.split(",")).map(x => (x(0), x(1)))

    val ds = spark.createDataset(rdd2)

    spark.stop()
  }
}
2. 自动推断

object RddToDataset {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RddToDataset").master("local").getOrCreate()

    val rdd = spark.sparkContext.textFile("file:///d:/data/words.txt")

    import spark.implicits._

    val ds = rdd.map {
      x => {
        val tmp = x.split(",")
        (tmp(0).toInt, tmp(1))
      }
    }.toDS()

    spark.stop()
  }
}
3. 反射获取schema

字段名已经通过反射获取。

object RddToDataset {

  case class Words(id: Long, name: String) extends Serializable

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RddToDataset").master("local").getOrCreate()

    val rdd = spark.sparkContext.textFile("file:///d:/data/words.txt")

    import spark.implicits._

    val ds = rdd.map {
      x => {
        val tmp = x.split(",")
        Words(tmp(0).toInt, tmp(1))
      }
    }.toDS()

    spark.stop()
  }
}
 

3. DataFrame/Dataset 转RDD
Dataset 转RDD

通过 .rdd，Dataset直接转即可。

object DatasetToRdd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DatasetToRdd")
        .master("local").getOrCreate()

    val rdd = spark.sparkContext.textFile("file:///d:/data/words.txt")

    import spark.implicits._

    val ds = rdd.map( x => {val tmp = x.split(",");(tmp(0).toInt, tmp(1))}).toDS()

    // 注意RDD类型
    val rdd2: RDD[(Int, String)] = ds.rdd

    spark.stop()
  }
}
DataFrame 转RDD

通过 .rdd，但是DataFrame转换后的是RDD[Row]，需要通过map转换为RDD[String]，或者直接通过 .getAs()获取。 

object DataFrameToRdd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameToRdd")
        .master("local").getOrCreate()

    val df = spark.read.text("file:///d:/data/words.txt")

    // RDD类型Row
    val rdd: RDD[Row] = df.rdd
    val rdd2: RDD[String] = rdd.map( _(0).toString)
    val rdd4: RDD[String] = rdd.map(_.get(0).toString)
    val rdd3: RDD[String] = rdd.map(_.getAs[String](0))

    spark.stop()
  }
}
 
4. DataFrame转Dataset
object DataFrameToDataset {

  case class Words(id: Long, name: String) extends Serializable

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameToDataset")
        .master("local").getOrCreate()

    val df = spark.read.text("file:///d:/data/words.txt")

    import spark.implicits._

    val ds = df.as[Words]

    spark.stop()
  }
}
 
5. Dataset转DataFrame
直接转即可，.toDF()。

object DatasetToDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DatasetToDataFrame")
        .master("local").getOrCreate()

    val rdd = spark.sparkContext.textFile("file:///d:/data/words.txt")

    import spark.implicits._

    val rdd2 = rdd.map(_.split(",")).map(x => (x(0), x(1)))

    val ds = spark.createDataset(rdd2)

    val df = ds.toDF

    spark.stop()
  }
}
 

 */