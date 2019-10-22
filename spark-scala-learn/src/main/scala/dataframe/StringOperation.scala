package dataframe

import dataframe.Constants._
import org.apache.spark.sql.functions._

object StringOperation extends App {

  df.show(5, false)
  //Converting first letter to upper case and other in lower case
  df.select(initcap(col("Description"))).show(5, false)

  //converting to lower and upper case
  df.select(col("Description"), lower(col("Description")),
    upper(lower(col("Description")))).show(5, false)

  //removing spaces around a string using lpad, rpad, ltrim, rtrim, trim
  df.select(
    ltrim(lit("    Hello  ")).as("ltrim"),
    rtrim(lit("    Hello  ")).as("rtrim"),
    trim(lit("     Hello  ")).as("trim"),
    lpad(lit("Hello"), 3, " ").as("lpad"),
    rpad(lit("Hello"), 5, " ").as("rpad")
  ).show(2)

  //extracting and replacing strings. Replaced all colors with COLOR
  val simpleColor = Seq("red", "yellow", "white", "green", "orange")
  val regex = simpleColor.map(_.toUpperCase).mkString("|")
  df.select(
    regexp_replace(col("Description"), regex, "COLOR").alias("color_clean"),
    col("Description")).show(2)

  //replacing characters using translate function.
  df.select(
    translate(col("Description"), "ENAMEL", "1729").alias("New Description"),
    col("Description")).show(4, false)


}
