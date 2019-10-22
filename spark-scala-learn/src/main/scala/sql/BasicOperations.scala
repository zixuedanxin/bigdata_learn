package sql

import org.apache.spark.sql.SparkSession

object BasicOperations extends App {


  val spark = SparkSession.builder().master("local[*]").appName("spark-sql").getOrCreate()

  //reading data as dataFrame
  val df = spark.read.format("json").load("src/main/resources/2015-summary.json")

  df.show(false)
  //Df => Sql
  df.createOrReplaceTempView("dfTable")

  spark.sql("""
     select DEST_COUNTRY_NAME, sum(count) from dfTable group By DEST_COUNTRY_NAME
     """.stripMargin)
    .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")
    .show() // SQl => DF

  // creating a table from json file

  spark.sql("""
      create table flights (
      DEST_COUNTRY_NAME string, ORIGIN_COUNTRY_NAME string, count long))
      using json options(path, 'src/main/resources/2015-summary.json')
    """.stripMargin).show(5)
}
