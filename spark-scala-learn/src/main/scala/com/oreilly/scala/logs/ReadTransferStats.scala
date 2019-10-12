/**
 * Illustrates reading in transfer statistics.
 */
package com.oreilly.scala.logs

import org.apache.hadoop.io.{IntWritable, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._


object ReadTransferStats {
  def readStats(ssc: StreamingContext, inputDirectory: String): DStream[(Long, Int)] = {
    // convert the input from Writables to native types
    ssc.fileStream[LongWritable, IntWritable,
      SequenceFileInputFormat[LongWritable, IntWritable]](inputDirectory).map{
      case (x, y) => (x.get(), y.get())
    }
  }
}
