package dataframe

import dataframe.Constants._
import org.apache.spark.sql.functions._

object ComplexOperation extends App {

  /*Types of Complex Operations
     Struct
     Array
     Map
   */

  //===================================Struct=============================================
  /* Creating a struct by wrapping set of columns in parenthesis
   */
  df.selectExpr("(Description, InvoiceNo) as complex", "*").show(2, false)

  //another solution
  val complexDf = df.select(struct("Description", "InvoiceNo").alias("complex"))
  complexDf.show(2)

  //to get a description from complex df
  complexDf.select("complex.Description").show(2, false)

  //another solution to get field as Invoice No
  complexDf.select(col("complex").getField("InvoiceNo")).show(2, false)

  //===================================Array=============================================

  //Convert description column to an array using applying split on rows.
  val array_col = df.select(split(col("Description"), " ").alias("array_col"))

  array_col.show(5, false)

  //selecting first element
  array_col.selectExpr("array_col[0]").show(5, false)

  //length of an array of each row
  array_col.select(col("array_col"), size(col("array_col")).as("length")).show(3, false)

  //checking whether array contains a specified value or not
  array_col.select(col("array_col"), array_contains(col("array_col"), "WHITE")).show(3, false)

  /* explode function -> takes a column that consists of arrays and creates one row per value in the aaray.  */

  df.withColumn("splitted", split(col("Description"), " "))
    .withColumn("exploded", explode(col("splitted")))
    .select("Description", "InvoiceNo", "exploded")
    .show(4, false)

  //===================================Map=============================================

  //Maps are created using map function. Selecting field from map is same as on array.
  val complexMap = df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
  complexMap.show(2, false)

  //querying the map
  complexMap.selectExpr("complex_map['WHITE METAL LANTERN']").show(2, false)

  //exploding map
  complexMap.select(explode(col("complex_map"))).show(4, false)

}
