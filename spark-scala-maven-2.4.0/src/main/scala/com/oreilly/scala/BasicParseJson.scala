/**
 * Illustrates a simple map partition to parse JSON data in Scala
 * Loads the data into a case class with the name and a boolean flag
 * if the person loves pandas.
 */
package com.oreilly.scala

import org.apache.spark._
import play.api.libs.json._

object BasicParseJson {
  case class Person(name: String, lovesPandas: Boolean)
  implicit val personReads = Json.format[Person]

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: [sparkmaster] [inputfile] [outputfile]")
      //exit(1)
      }
    val master = "local"
    val inputFile = "data/pandainfo.json"
    val outputFile = "/home/xzh/test2"
    val sc = new SparkContext(master, "BasicParseJson", System.getenv("SPARK_HOME"))
    val input = sc.textFile(inputFile)
    val parsed = input.map(Json.parse(_))
    // We use asOpt combined with flatMap so that if it fails to parse we
    // get back a None and the flatMap essentially skips the result.
    val result = parsed.flatMap(record => personReads.reads(record).asOpt)

    result.filter(_.lovesPandas).map(Json.toJson(_)).foreach(println)//.saveAsTextFile(outputFile)
    }
}
