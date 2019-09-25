package sparkdemo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

object WindowFunctions {
  def main(args: Array[String]) {
    val spark =
      SparkSession.builder()
        .appName("Experiments")
        .master("local[4]")
        .getOrCreate()

    import spark.implicits._

    println("*** stepped range with specified partitioning")
    //val dfm = spark.range(10, 20, 2, 2)
    val df = Seq((1, "specificSensor", Some(1)),
      (2, "1234", None),
      (3, "1234", None),
      (4, "specificSensor", Some(2)),
      (5, "2345", None),
      (6, "2345", None))
      .toDF("id", "sensor", "no")
    df.show()
    val df2=df//.toDF(colNames = "id")
    println(df2.columns.mkString(","))
    df2.withColumn("status_lag",lit(Long.MaxValue)-$"id").show() //lag(col("id"),1)
    println("# Partitions num = " + df.rdd.partitions.length) //coalesce(lag("id", 1,99), lit(Long.MaxValue))
    val idWindow2 = Window.orderBy("id")
   df2.withColumn("prePrice",  lag("id", 1,999).over(idWindow2) ).show()
//    df2.show()
//    val ldf = df.select($"id", lag(col("id"), 1))

    //ldf.show()
    val df4 = Seq((1, "specificSensor", Some(1)),
      (2, "1234", None),
      (3, "1234", None),
      (4, "specificSensor", Some(2)),
      (5, "2345", None),
      (6, "2345", None))
      .toDF("ID", "Sensor", "No")
    val idWindow = Window.orderBy("ID")
    val sensorsRange = df4
      .where($"Sensor" === "specificSensor")
      .withColumn("nextId", coalesce(lead($"id", 1).over(idWindow), lit(Long.MaxValue)))

    sensorsRange.show(false)

    val joinColumn = $"d.ID" > $"s.id" && $"d.ID" < $"s.nextId"
    val result =
      df4.alias("d")
        .join(sensorsRange.alias("s"), joinColumn, "left")
        .select($"d.ID", $"d.Sensor", coalesce($"d.No", $"s.No").alias("No"))
    result.show()


  }

}
