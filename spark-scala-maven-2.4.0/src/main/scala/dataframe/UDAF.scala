package dataframe

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

//
// An example of the experimental User Defined Aggregation Function mechanism
// added in Spark 1.5.0.
//
object UDAF {

  //
  // A UDAF that sums sales over $500
  //
  private class ScalaAggregateFunction extends UserDefinedAggregateFunction {

    // an aggregation function can take multiple arguments in general. but
    // this one just takes one 输入的数据类型
    def inputSchema: StructType =
      new StructType().add("sales", DoubleType)
    // the aggregation buffer can also have multiple values in general but
    // this one just has one: the partial sum  缓存区的数据类型
    def bufferSchema: StructType =
      new StructType().add("sumLargeSales", DoubleType)
    // returns just a double: the sum  最终返回的数据类型
    def dataType: DataType = DoubleType
    // always gets the same result 决定每次相同输入，是否返回相同输出， 一般都会设置为
    def deterministic: Boolean = true

    // each partial sum is initialized to zero 缓存区初始化的数值
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0.0)
    }

    // an individual sales value is incorporated by adding it if it exceeds 500.0 每行数据都会处理
    // 这个是组类根据自己的逻辑进行拼接， 然后更新数据
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sum = buffer.getDouble(0)
      if (!input.isNullAt(0)) {
        val sales = input.getDouble(0)
        if (sales > 500.0) {
          buffer.update(0, sum+sales)
        }
      }
    }

    // buffers are merged by adding the single values in them
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
    }

    // the aggregation buffer just has one value: so return it
    def evaluate(buffer: Row): Any = {
      buffer.getDouble(0)
    }
  }

  def main (args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("DataFrame-UDAF")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    // create an RDD of tuples with some data
    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 200.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )
    val customerRows = spark.sparkContext.parallelize(custs, 4)
    val customerDF = customerRows.toDF("id", "name", "sales", "discount", "state")
    val mysum = new ScalaAggregateFunction()
    customerDF.printSchema()
    customerDF.show()
    val results = customerDF.groupBy("state").agg(mysum($"sales").as("bigsales"))
    results.printSchema()
    results.show()

  }

}
