package dataframe

import dataframe.Constants._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object AggregationOperation extends App {

  //let's minimize the partition
  val modifiedDf: DataFrame = df.coalesce(5).toDF()
  modifiedDf.cache()

  modifiedDf.show(false)

  //counting the number of rows
  println(s"number of rows in dataframe: ${modifiedDf.count()}")

  //counting number of rows for particular column
  modifiedDf.select(count(col("StockCode"))).show(1)

  //number of unique rows. It als ignores null rows.
  modifiedDf.select(countDistinct(col("StockCode"))).show(1)

  //Sometimes we want approximate distinct count
  modifiedDf.select(approx_count_distinct(col("StockCode"), 0.1)).show(1)

  //To extract first and last value fom a DataFrame
  modifiedDf.select(first(col("StockCode")), last(col("StockCode"))).show(1)

  //To extract the min and max value fom a DataFrame
  modifiedDf.select(max(col("Quantity")), min(col("Quantity"))).show(1)

  //To sum all values in a row
  modifiedDf.select(sum(col("Quantity"))).show(1)

  //To sum only distinct values
  modifiedDf.select(sumDistinct(col("Quantity"))).show(1)

  //To find the average
  modifiedDf.select(avg(col("Quantity"))).show(1)

  // To find average and mean
  modifiedDf.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchase"),
    avg("Quantity").alias("avg_purchase"),
    expr("mean(Quantity)").alias("mean_purchase"))
    .selectExpr(
      "total_purchase/total_transactions",
      "avg_purchase",
      "mean_purchase"
    ).show(1)

  //Finding standard deviation and variance
  modifiedDf.select(stddev_pop(col("Quantity")),
    stddev_samp(col("Quantity")),
    var_pop(col("Quantity")),
    var_samp(col("Quantity"))
  ).show(1)

  //Finding asymmetry around mean. Skewness and kurtosis

  modifiedDf.select(skewness(col("Quantity")), kurtosis(col("Quantity"))).show(1)

  // finding correlation and covariance between two columns
  modifiedDf.select(corr("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity"),
    covar_samp("InvoiceNo", "Quantity")
  ).show(1)

  //Aggregating to complex type
  modifiedDf.select(collect_list("InvoiceNo"), collect_set("Quantity")).show(1)

  //Group InvoiceNo, CustomerId and get the count
  modifiedDf.groupBy("InvoiceNo", "CustomerId").count().show(2)


  modifiedDf.groupBy("InvoiceNo").agg(
    count("Quantity").alias("Quant"),
    expr("count(Quantity)")
  ).show(2)

  //Grouping with Maps
  modifiedDf.groupBy("InvoiceNo").agg("Quantity" -> "avg", "Quantity" -> "stddev_pop").show(2)

  /*
    Window function: It returns the value for over a particular frame of data. First add date to it.
   */

  val dfWithDate = modifiedDf.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
/*
  Created window
 */
  val window = Window.partitionBy("CustomerId", "date")
    .orderBy(col("Quantity").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  //max purchase quantity over that window
  val maxPurchaseQuantity: Column = max(col("Quantity")).over(window)
  val purchaseDenseRank = dense_rank()
    .over(window)
  val purchaseRank = rank().over(window)

  dfWithDate.where("CustomerId is not null").orderBy("CustomerId")
    .select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDenseRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity")
  ).show(4)

  /*
    Arbitrary aggregations: Aggregation across multiple groups.
    Get the total quantity of all stock codes and customer.
    Grouping set depend on null values for aggregation levels. Always first filter out null values.
   */

  dfWithDate.drop()
    .groupBy("StockCode", "CustomerId")
    .agg(sum(col("Quantity")).as("sum"))
    .select("CustomerId", "StockCode", "sum")
    .orderBy(col("CustomerId").desc, col("StockCode").desc).show()

  /*
  rollup -> multidimensional aggregation that performs a variety of groups-by-style calculation for us.
  Calculate the grand total of all dates and total of each date in the dataFrame and the subtotal for
  each country on each date in the dataFrame
   */

  dfWithDate.drop().rollup("date", "Country").agg(sum("Quantity"))
    .selectExpr("date", "Country", "`sum(Quantity)`")
    .orderBy("date")
    .show()

  /*
  Finally cube: it takes a rollup to a level deeper.
  Find following in 1 query
  The total across all dates and countries.
  The total for each date across all country.
  The total for each country on each date.
  The total for each country across all dates.
  */

  dfWithDate.drop().cube("date", "Country")
    .agg(sum(col("Quantity")))
    .select("date", "Country", "`sum(Quantity)`")
    .orderBy("date").show()

  /*
     Want to query the rollup/cube results?
   */

  dfWithDate.show(5)

  dfWithDate.cube("CustomerId", "StockCode")
    .agg(grouping_id(), sum("Quantity")).orderBy(desc("grouping_id()"))
    .show()

  // Pivot:  Converting a row to column
  val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
  pivoted.show()
  pivoted.where("date > '2010-11-5'").select("date", "`Australia_sum(Quantity)`").show(4)
}

/*
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
 //scala实现row_number() over(partition by  , order by  )
 val w = Window.partitionBy($"prediction").orderBy($"count".desc)
 val dfTop3= dataDF.withColumn("rn", row_number().over(w)).where($"rn" <= 3).drop("rn")
 http://spark.apache.org/docs/2.4.4/api/scala/index.html#org.apache.spark.sql.expressions.Window

 */