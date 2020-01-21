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
import java.io.PrintWriter
import java.net.ServerSocket
import java.text.SimpleDateFormat
import java.util.{Date, Random}

/** Represents a page view on a website with associated dimension data. */
class PageView(val url: String, val status: Int, val zipCode: Int, val userID: Int,val oper_dtm:String)
    extends Serializable {
  override def toString(): String = {
    "%s\t%s\t%s\t%s\t%s\n".format(url, status, zipCode, userID,oper_dtm)
  }
}

object PageView extends Serializable {
  def fromString(in: String): PageView = {
    val parts = in.split("\t")
    new PageView(parts(0), parts(1).toInt, parts(2).toInt, parts(3).toInt,parts(4).toString)
  }
}

// scalastyle:off
/**
 * Generates streaming events to simulate page views on a website.
 *
 * This should be used in tandem with PageViewStream.scala. Example:
 *
 * To run the generator
 * `$ bin/run-example org.apache.spark.examples.streaming.clickstream.PageViewGenerator 44444 10`
 * To process the generated stream
 * `$ bin/run-example \
 *    org.apache.spark.examples.streaming.clickstream.PageViewStream errorRatePerZipCode localhost 44444`
 *
 */
// scalastyle:on
object soket_data {
  val pages = Map("foo.com" -> .2,
                  "baidu.com" -> .2,
                  "qq.com" -> .2,
                  "java"->0.1,
                  "python"->0.3)
  val httpStatus = Map(200 -> .95,
                       404 -> .05)
  val userZipCode = Map(10001 -> .5,
                        10002  -> .5)
  val userID = Map((1 to 5).map(_ -> .2): _*)

  def pickFromDistribution[T](inputMap: Map[T, Double]): T = {
    val rand = new Random().nextDouble()
    var total = 0.0
    for ((item, prob) <- inputMap) {
      total = total + prob
      if (total > rand) {
        return item
      }
    }
    inputMap.take(1).head._1 // Shouldn't get here if probabilities add up to 1.0
  }

  def getNextClickEvent(): String = {
    val id = pickFromDistribution(userID)
    val page = pickFromDistribution(pages)
    val status = pickFromDistribution(httpStatus)
    val zipCode = pickFromDistribution(userZipCode)
    val oper_dtm=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    new PageView(page, status, zipCode, id,oper_dtm).toString()
  }

  def main(args: Array[String]) {
//    if (args.length != 2) {
//      //System.err.println("Usage: PageViewGenerator <port> <viewsPerSecond>")
//     // System.exit(1)
//    }
    val port =3000// args(0).toInt
    // val viewsPerSecond =10.0// 0.5 //args(1).toFloat
    val sleepDelayMs = 30000//(1000.0 / viewsPerSecond).toInt 10000表示10秒
    val listener = new ServerSocket(port)
    println("Listening on port: " + port)

    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run(): Unit = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)

          while (true) {
            print("新增：")
            Thread.sleep(sleepDelayMs)
            val tp=getNextClickEvent()
            println(tp)
            out.write(tp)
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
// scalastyle:on println
