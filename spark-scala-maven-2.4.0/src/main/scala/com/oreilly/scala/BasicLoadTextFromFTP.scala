/**
 * Illustrates loading a text file from FTP
 */
package com.oreilly.scala

import org.apache.spark._

object BasicTextFromFTP {
    def main(args: Array[String]) {
      val conf = new SparkConf
      conf.setMaster(args(0))
      val sc = new SparkContext(conf)
      val file = sc.textFile("ftp://anonymous:pandamagic@ftp.ubuntu.com/ubuntu/ls-LR.gz")
      println(file.collect().mkString("\n"))
    }
}
