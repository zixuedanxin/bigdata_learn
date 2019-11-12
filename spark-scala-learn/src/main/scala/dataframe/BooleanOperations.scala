package dataframe

import dataframe.Constants._
import org.apache.spark.sql.functions._

object BooleanOperations extends App {


  df.show()

  df.select(lit(1), lit("one"), lit(5.0)).show()

  //filter our df on the basis of invoice no
  df.where(col("InvoiceNo").equalTo("536369"))
    .select("InvoiceNo", "Description")
    .show(5, false)

  //another solution using ===
  df.where(col("InvoiceNo") === "536369")
    .select("InvoiceNo", "Description")
    .show(5, false)

  //using expression and not equal to condition
  df.where("InvoiceNo <> 536369")
    .show(5, false)

  //filtering many columns
  val priceFilter = col("UnitPrice") > 600
  val descriptionFilter = col("Description").contains("POSTAGE")

  df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descriptionFilter)).show(10, false)

  //filtering and assigning result to new column

  val dotFilter = col("StockCode") === "DOT"

  df.withColumn("isExpensive", dotFilter.and(priceFilter.or(descriptionFilter)))
    .where("isExpensive")
    .select("unitPrice", "isExpensive").show(5)

  //not equal condition
  df.withColumn("isExpensive", not(col("unitPrice").leq(250)))
    .filter("isExpensive")
    .select("InvoiceNo", "Description")
    .show(5, false)

  //avoiding null
  df.where(col("Description").eqNullSafe("Hello")).show(5)
}

