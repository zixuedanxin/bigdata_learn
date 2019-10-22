package dataframe

import dataframe.Constants.spark
import org.apache.spark.sql.functions._

object DateTimeOperations extends App {

  val dateDf = spark.range(10)
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())

   dateDf.printSchema()

  //Add and Subtract 3 days to our data frame
  dateDf.select(
    col("today"),
    date_add(col("today"), 3).alias("Three Days Ago"),
    date_sub(col("today"), 3).alias("Three Days After")).show(1)

  //Find the difference between two given dates. datediff will give the difference.
  dateDf
    .withColumn("week_ago", date_sub(col("today"), 7))
    .select(datediff(col("week_ago"),  col("today"))).show(1)

  /*Find the difference between two months. months_between will do that.
    to_date will convert string to date
   */
  dateDf.select(
    to_date(lit("2019-06-14")).alias("start"),
    to_date(lit("2019-08-30")).alias("end"))
    .select(months_between(col("start"), col("end"))).show(1)

  spark.range(5)
    .withColumn("date", lit("2019-08-30"))
    .select(to_date(col("date"))).show(1)

  /*
  If invalid date formate is given, Spark will never throw exception. It will just
  return null.
   */
  dateDf.select(
      to_date(lit("2019-14-06")).alias("Invalid Format"),
      to_date(lit("2019-06-14")).alias("Valid Format")
  ).show(1)

  /*
    To solve such date format problems. Provide date format for safety.
   */
  val dateFormat = "yyyy-dd-MM"
  val cleanedDf =
  spark.range(1).select(
    to_date(lit("2019-14-06"), dateFormat).alias("date"),
    to_date(lit("2019-20-12"), dateFormat).alias("date2")
  )

  cleanedDf.show(1)

  // To use a timestamp, always provide timestamp
  cleanedDf.select(to_timestamp(col("date"), dateFormat)).show(1)

  cleanedDf.filter(col("date2") > lit("2019-12-12")).show(1)
}
