package sljr.ybd.spark.test
import java.util
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{Accumulable, Accumulator, SparkContext, SparkConf}
import org.apache.spark.broadcast.Broadcast

/**
  * Created by lxh on 2016/6/30.
  */
object BroadcastAccumulatorStreaming {

  /**
    * 声明一个广播和累加器！
    */
  private var broadcastList:Broadcast[List[String]]  = _
  private var accumulator:Accumulator[Int] = _

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("broadcasttest")
    val sc = new SparkContext(sparkConf)

    /**
      * duration是ms
      */
    val ssc = new StreamingContext(sc,Duration(2000))
    // broadcastList = ssc.sparkContext.broadcast(util.Arrays.asList("Hadoop","Spark"))
    broadcastList = ssc.sparkContext.broadcast(List("Hadoop","Spark"))
    accumulator= ssc.sparkContext.accumulator(0,"broadcasttest")
    /**
      * 获取数据！
      */
    val lines = ssc.socketTextStream("localhost",9999)

    /**
      * 拿到数据后 怎么处理！
      *
      * 1.flatmap把行分割成词。
      * 2.map把词变成tuple(word,1)
      * 3.reducebykey累加value
      * (4.sortBykey排名)
      * 4.进行过滤。 value是否在累加器中。
      * 5.打印显示。
      */
    val words = lines.flatMap(line => line.split(" "))
    val wordpair = words.map(word => (word,1))
    wordpair.filter(record => {broadcastList.value.contains(record._1)})
    val pair = wordpair.reduceByKey(_+_)
    /**
      *这步为什么要先foreachRDD？
      *
      * 因为这个pair 是PairDStream<String, Integer>
      *
      *   进行foreachRDD是为了？
      *
      */
    /*    pair.foreachRDD(rdd => {
          rdd.filter(record => {

            if (broadcastList.value.contains(record._1)) {
              accumulator.add(1)
              return true
            } else {
              return false
            }

          })

        })*/

    val filtedpair = pair.filter(record => {
      if (broadcastList.value.contains(record._1)) {
        accumulator.add(record._2)
        true
      } else {
        false
      }
    }).print

    println("累加器的值"+accumulator.value)
    // pair.filter(record => {broadcastList.value.contains(record._1)})

    /* val keypair = pair.map(pair => (pair._2,pair._1))*/

    /**
      * 如果DStream自己没有某个算子操作。就通过转化transform！
      */
    /* keypair.transform(rdd => {
       rdd.sortByKey(false)//TODO
     })*/
    pair.print()
    ssc.start()
    ssc.awaitTermination()

  }

}