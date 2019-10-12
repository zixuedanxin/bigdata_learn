/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples

import java.util.Date

import scala.math.random

import java.util.Random
import org.apache.spark. {SparkConf, SparkContext}
import org.apache.spark.SparkContext._
/**
  * Usage: GroupByTest [numMappers] [numKVPairs] [valSize] [numReducers]
  */
object LocalPi {
  def main ( args : Array[String]) {
    val sparkConf = new SparkConf().setAppName ( "GroupBy Test" ).setMaster("local")
    var numMappers = 100
    var numKVPairs = 10000
    var valSize = 1000
    var numReducers = 36
    val sc = new SparkContext( sparkConf )
    val pairs1 = sc.parallelize(0 until numMappers,numMappers).flatMap{p=>    //flatMap(_=>print(p))

      val ranGen = new Random
      var arr1 = new Array[(Int ,Array[Byte])](numKVPairs)
      for(i <- 0 until numKVPairs)
      {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) =(ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache
    // Enforce that everything has been calculated and in cache
    println(pairs1.toDebugString)
    pairs1.count
    println(pairs1.groupByKey(numReducers). count )
    sc.stop ()
  }
}

//object LocalPi {
//  def main(args: Array[String]) {
//    var start_time =new Date().getTime
//    var count = 0
//    for (i <- 1 to 1000000) {
//      val x = random * 2 - 1
//      val y = random * 2 - 1
//      if (x*x + y*y <= 1) count += 1
//    }
//    println("Pi is roughly " + 4 * count / 100000.0)
//    var end_time =new Date().getTime
//    println(end_time-start_time) //单位毫秒
//  }
//}
// scalastyle:on println
