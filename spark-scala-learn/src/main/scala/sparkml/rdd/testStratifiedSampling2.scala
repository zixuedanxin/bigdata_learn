package sparkml.rdd

import org.apache.spark.{SparkConf, SparkContext}


object testStratifiedSampling2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("testStratifiedSampling2")
    val sc = new SparkContext(conf)
    val data = sc.textFile("a.txt").map(row => {
      if (row.length == 3)
        (row, 1)
      else
        (row, 2)
    }
    ).map(each => (each _2, each _1))
    val fractions : Map[Int, Double] = (List((1,0.3),(2,0.7))).toMap//设定抽样格式,fractions表示在层1抽0.2，在层2中抽0.8
    //withReplacement false表示不重复抽样
    val approxSample = data.sampleByKey(withReplacement = false, fractions, 0)
    approxSample.foreach(println)
  }
}
