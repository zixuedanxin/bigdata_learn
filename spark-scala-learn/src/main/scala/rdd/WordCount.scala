package rdd

import org.apache.spark.{SparkConf, SparkContext}
/*
  Simple Word Count Example
 */
object WordCount extends App {
  val conf = new SparkConf().setMaster("local").setAppName("Word-Count")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val lines = sc.textFile("src/main/resources/sample")
  val words = lines.flatMap(line => line.split(" "))
  val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }

  println("foreach over RDD will give output in random order")
  counts.foreach(println)
  println("foreach over collect will output in same order of textfile")
  counts.collect().foreach(println)
  sc.stop()
}
