/**
 * Illustrates intersection by key
 */
package com.oreilly.scala

import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object BasicIntersectByKey {

  def intersectByKey[K: ClassTag, V: ClassTag](rdd1: RDD[(K, V)], rdd2: RDD[(K, V)]): RDD[(K, V)] = {
    rdd1.cogroup(rdd2).flatMapValues{
      case (Nil, _) => None
      case (_, Nil) => None
      case (x, y) => x++y
    }
  }

  def main(args: Array[String]) {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "BasicIntersectByKey", System.getenv("SPARK_HOME"))
    val rdd1 = sc.parallelize(List((1, "panda"), (2, "happy")))
    val rdd2 = sc.parallelize(List((2, "pandas")))
    val iRdd = intersectByKey(rdd1, rdd2)
    iRdd.collect().foreach(println)
    val panda: List[(Int, String)] = iRdd.collect().toList
    panda.map(println(_))
    sc.stop()
  }
}
