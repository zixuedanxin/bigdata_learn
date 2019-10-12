package sljr.ybd.spark.test.hive
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
object hive_sql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Hive Example")  //.config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("SELECT COUNT(*) FROM sdd.xfqz_user").show()
  }
}