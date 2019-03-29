package com.bigdata.spark.sql.n_02_spark_dataset.action.n_04_dataset_head_n

import com.bigdata.spark.standalone.base.BaseSparkSession


object Run extends BaseSparkSession{


  def main(args: Array[String]): Unit = {


    val spark = sparkSession()

    val dataSet = spark.read.textFile("src/main/resource/data/text/people.txt")

    println(dataSet.head(10).mkString("\n"))
    import spark.implicits._
    val df=dataSet.map(x=>{val tp=x.split(",")
      (tp(0).trim,tp(1).trim.toInt)
    }).toDF("name","age")
    df.show()
    //df

    spark.stop()


  }
}

