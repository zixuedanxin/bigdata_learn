/**
  * Illustrates loading a text file of integers and counting the number of invalid elements
  */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark.util.{AccumulatorV2, CollectionAccumulator, DoubleAccumulator, LongAccumulator}
import org.apache.spark._
import org.apache.spark.SparkContext._

object BasicLoadNums {
  class  MyAccumulator extends AccumulatorV2[Int,Int]{

    private var res = 0

    override def isZero: Boolean = {res == 0}

    override def merge(other: AccumulatorV2[Int, Int]): Unit = other match {
      case o : MyAccumulator => res += o.res
      case _ => throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }

    override def copy(): MyAccumulator = {
      val newMyAcc = new MyAccumulator
      newMyAcc.res = this.res
      newMyAcc
    }

    override def value: Int = res

    override def add(v: Int): Unit = res += v // +"-"

    override def reset(): Unit = res = 0
  }

  def main(args: Array[String]) {
    val master = "local[2]"
    val inputFile = "data/graphx/followers.txt"
    val sc = new SparkContext(master, "BasicLoadNums", System.getenv("SPARK_HOME"))
    val file = sc.textFile(inputFile)
    val myint=new MyAccumulator

    def longAccumulator(name: String): LongAccumulator = {
      val acc = new LongAccumulator
      sc.register(acc, name)
      acc
    }

    def collectionAccumulator[T](name: String): CollectionAccumulator[T] = {
      val acc = new CollectionAccumulator[T]
      sc.register(acc, name)
      acc
    }

    def doubleAccumulator(name: String): DoubleAccumulator = {
      val acc = new DoubleAccumulator
      sc.register(acc, name)
      acc
    }

    val errorLines = longAccumulator("errorLines") // sc.accumulator(0) // Create an Accumulator[Int] initialized to 0
    val dataLines = longAccumulator("dataLines") //sc.accumulator(0)  // Create a second Accumulator[Int] initialized to 0
    sc.register(myint,"myAcc")
    val counts = file.flatMap(line => {
      try {
        val input = line.split(" ")
        //println(line)
        val data = Some((input(0), input(1).toInt))
        dataLines.add(input(1).toInt) //+= 1
        //println(dataLines.value)
        myint.add(1)
        data
      } catch {
        case e: java.lang.NumberFormatException => {
          errorLines.add(1)
          None
        }
        case e: java.lang.ArrayIndexOutOfBoundsException => {
          errorLines.add(1)
          None
        }
      }
    }).reduceByKey(_ + _)
    counts.foreach(println)
    println(errorLines.value, dataLines.count,dataLines.sum,myint.value)
//    if (errorLines.value < dataLines.value) {
//      counts.saveAsTextFile("output2.txt")
//    } else {
//      println(s"Too many errors ${errorLines.value} for ${dataLines.value}")
//    }
  }
}
