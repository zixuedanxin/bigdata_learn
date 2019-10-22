package dataframe

import dataframe.Constants._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object NumberOperations extends App {

  /*
  To modify the existing data. Recorded Quantity is incorrect. So update it.
   quantity = (current quantity * unitPrice)^2 + 5
   */

  val modifiedQuantity: Column = pow(col("Quantity") * col("UnitPrice"), 2) + 5

  df.select(expr("CustomerId"), modifiedQuantity.as("realQuantity")).show(2)

  //rounding off decimal values
  df.select(round(col("UnitPrice"), 1).as("rounded"), col("UnitPrice")).show(5)

  //2.5 -> round will choose 3 but if lower value is needed, bround function comes to the picture
  df.select(round(lit("2.5")), bround(lit("2.5"))).show(5)

  //find correlation of two columns use corr method
  df.select(corr("Quantity", "UnitPrice")).show(2)

  // Sumarise stats of set of columns
  df.summary().show(3, false)

  //adding  unique id to dataframe
  df.select(monotonically_increasing_id()).show(5)
}
