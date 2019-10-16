package streamings

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 必须以流式方式写入数据
  * 比如在windows中使用 “echo "hello world >> xx.txt"”
  */
object FileWordCount {
  def main(args:Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FileWordCount");
    val ssc = new StreamingContext(sparkConf, Seconds(15))
    val lines = ssc.textFileStream("/home/xzh/data/")//需要监控的目录
    lines.print()
    //lines = null
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
