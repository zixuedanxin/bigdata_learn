package rdd

import org.apache.spark.{SparkConf, SparkContext}

object Ex9_Scraps {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Ex9_Scraps").setMaster("local[4]")
    val sc = new SparkContext(conf)
     sc.setLogLevel("WARN")
    // look at the distribution of numbers across partitions
    val numbers =  sc.parallelize(1 to 100, 4)


    // preferredLocations -- not too interesting right now
    numbers.partitions.foreach(p => {
      println("Partition: " + p.index)
      numbers.preferredLocations(p).foreach(s => println("  Location: " + s))
    })

    // TODO: Partitioner

    // TODO: PairRDDFunctions

  }
}
