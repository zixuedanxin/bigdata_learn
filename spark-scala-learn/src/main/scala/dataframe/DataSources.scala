package dataframe

import dataframe.Constants.spark
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object DataSources extends App {

  //reading a csv file
  val csvDf = spark
    .read
    .format("csv")
    .option("mode", "failfast") // using to specify how much tolerance we have for malformed data
    .option("header", "true")
    .option("inferschema", "true")
    .load("src/main/resources/retailer.csv")

  csvDf.show(2)

  //writing csv file as tsv
  csvDf
    .write
    .format("csv")
    .mode("overwrite")
    .option("sep", "\t")
    .save("src/main/tmp/retailer.csv")

  val jsonDf =
    spark
    .read
    .format("json")
    .option("mode", "failfast")
    .load("src/main/resources/2015-summary.json")

  jsonDf.show()

  //saving csv file as json
  csvDf.write.format("json").mode("overwrite").save("src/main/tmp/retailer.json")

  //saving json file as parquet // default mode is parquet format
  csvDf.write.mode("overwrite").save("src/main/tmp/retailer.parquet")

  spark
    .read
    .format("parquet")
    .option("mode", "failfast")
    .load("src/main/tmp/retailer.parquet").show()
}
