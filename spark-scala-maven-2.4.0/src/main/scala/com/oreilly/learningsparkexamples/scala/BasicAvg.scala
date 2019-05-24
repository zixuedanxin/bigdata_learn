/**
 * Illustrates a simple aggregate in scala to compute the average of an RDD
 */
package com.oreilly.learningsparkexamples.scala

import org.apache.spark._
import org.apache.spark.rdd.RDD

object BasicAvg {
  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "BasicAvg", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(List(1,2,3,4))
    val result = computeAvg(input)
    val avg = result._1 / result._2.toFloat
    println(result)

    val data = List(2,5,8,1,2,6,9,4,3,5)
    val res = data.par.aggregate((0,0))(
      // seqOp
      (acc, number) => (acc._1+number, acc._2+1),
      // combOp
      (par1, par2) => (par1._1+par2._1, par1._2+par2._2)
    )

    println(res)

    val data2 = List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8))
    val rdd = sc.parallelize(data2)

    val res2 : RDD[(Int,Int)] = rdd.aggregateByKey(0)(
      // seqOp
      math.max(_,_),
      // combOp
      _+_
    )

    res2.collect.foreach(println)

    val res3 : RDD[(Int,Int)] = rdd.aggregateByKey(0)(
      // seqOp
      _+_,
      // combOp
      _+_
    )

    res3.collect.foreach(println)

    val res4 : RDD[(Int,(Int,Int))] = rdd.aggregateByKey((0,0))(
      // seqOp
      (x,y)=> (x._1+y,x._2+1),
      // combOp
      (x,y)=>(x._1+y._1,x._2+y._2)
    )

    res4.collect.foreach(println)

    sc.stop
  }
  def computeAvg(input: RDD[Int]) = {
    input.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1),
      (x,y) => (x._1 + y._1, x._2 + y._2))
  }
}
