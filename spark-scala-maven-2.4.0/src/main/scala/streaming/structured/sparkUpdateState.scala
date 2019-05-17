package streaming.structured

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils

object sparkUpdateState {
  def main(args: Array[String]): Unit = {
    //由于日志信息较多，只打印错误日志信息
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("dstream").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    //使用updateStateByKey前需要设置checkpoint，将数据进行持久化保存，不然每次执行都是新的，不会与历史数据进行关联
    //    ssc.checkpoint("f:/spark_out")
    //将数据保存在hdfs中
    ssc.checkpoint("/tmp/spark_out")
    //与kafka做集成，使用KafkaUtils类进行参数配置
//    val(zkQuorum,groupId,topics)=("192.168.200.10:2181","kafka_group",Map("sparkTokafka"->1))
//    val value: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorum,groupId,topics)
    val lines = ssc.socketTextStream("localhost", 3000)
    //lines.foreachRDD(x=> x.foreach(s=>println(s._1)))
    val words =lines.map(x=>(x.split("\t")(0),1)) //lines.flatMap(_.split("\t")(0))
    // words.print()
    //val wordDstream = words.map(x => (x, 1))
    //将updateFunc转换为算子
    val updateFunc2 = updateFunc _
    //统计输入的字符串，根据空格进行切割统计
   // value.flatMap(_._2.split(" ")).map((_,1)).updateStateByKey[Int](updateFunc2).print()
    words.updateStateByKey[Int](updateFunc2).print()
    ssc.start()
    ssc.awaitTermination()
  }
  def updateFunc(seq:Seq[Int], option:Option[Int])={
    //sum统计一次批处理后，单词统计
    var sum=seq.sum;
    //i为已经累计的值，因为option可能为空，如果为空的话，返回的是None，所以如果为空则返回0
    val i = option.getOrElse(0)
println(seq,i)
    // 返回累加后的结果，是一个Option[Int]类型
    Some(sum+i)
  }
}
