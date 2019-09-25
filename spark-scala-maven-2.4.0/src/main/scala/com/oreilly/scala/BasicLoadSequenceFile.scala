/**
 * Loads a simple sequence file of people and how many pandas they have seen.
 */
package com.oreilly.scala

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark._


object BasicLoadSequenceFile {
    def main(args: Array[String]) {
      val master = args(0)
      val inFile = args(1)
      val sc = new SparkContext(master, "BasicLoadSequenceFile", System.getenv("SPARK_HOME"))
      val data = sc.sequenceFile(inFile, classOf[Text], classOf[IntWritable]).map{case (x, y) =>
        (x.toString, y.get())}
      println(data.collect().toList)
    }
}
