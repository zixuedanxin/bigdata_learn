package streamings.structured

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils

object sparkUpdateState {
  def main(args: Array[String]): Unit = {
    //由于日志信息较多，只打印错误日志信息
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("dstream").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(10))
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
    //words.print()
    //val wordDstream = words.map(x => (x, 1))
    //将updateFunc转换为算子
    val updateFunc2 = updateFunc _
    //统计输入的字符串，根据空格进行切割统计
   // value.flatMap(_._2.split(" ")).map((_,1)).updateStateByKey[Int](updateFunc2).print()
    // words.updateStateByKey[Int](updateFunc2).print()

    // 使用UpdateStateByKey进行更新
    val result = words.updateStateByKey((seq:Seq[Int],option:Option[Int])=>{
      // 初始化一个变量
      var value = 0;
      // println(option)
      // 该变量用于更新，加上上一个状态的值，这里隐含一个判断，如果有上一个状态就获取，如果没有就赋值为0
      value += option.getOrElse(0)

      // 遍历当前的序列，序列里面每一个元素都是当前批次的数据计算结果，累加上一次的计算结果
      for(elem <- seq){
        value +=elem
        println("for loop: ",elem,value)
      }

      // 返回一个Option对象
      Option(value)
    })
    // 累加，打印
    result.print()
//    val wordcount1 = result.reduceByKey(_ + _)
//    wordcount1.print()

    ssc.start()
    ssc.awaitTermination()
  }
  def updateFunc(seq:Seq[Int], option:Option[Int]): Some[Int] ={
    //sum统计一次批处理后，单词统计
    var sum=seq.sum;
    //i为已经累计的值，因为option可能为空，如果为空的话，返回的是None，所以如果为空则返回0
    val i = option.getOrElse(0)
    // println(seq,i)
    // 返回累加后的结果，是一个Option[Int]类型
    Some(sum+i)
  }

//  def mappingFunction(key:String,value:Option[Int],state:State[Long]):(String,Long) =
//    {
//            // 获取之前状态的值
//            val oldState= state.getOption().getOrElse(0L)
//            // 计算当前状态值
//            val newState= oldState + value.getOrElse(0)
//            // 更新状态值
//            state.update(newState)
//            // 返回结果
//            (key,newState)
//      }

}

/*
updateStateBykey函数有6种重载函数：
1、只传入一个更新函数，最简单的一种。
更新函数两个参数Seq[V], Option[S]，前者是每个key新增的值的集合，后者是当前保存的状态，
def updateStateByKey[S: ClassTag](
    updateFunc: (Seq[V], Option[S]) => Option[S]
  ): DStream[(K, S)] = ssc.withScope {
  updateStateByKey(updateFunc, defaultPartitioner())
}
例如，对于wordcount，我们可以这样定义更新函数：
(values:Seq[Int],state:Option[Int])=>{
  //创建一个变量，用于记录单词出现次数
  var newValue=state.getOrElse(0) //getOrElse相当于if....else.....
  for(value <- values){
    newValue +=value //将单词出现次数累计相加
  }
  Option(newValue)
}
2、传入更新函数和分区数
def updateStateByKey[S: ClassTag](
    updateFunc: (Seq[V], Option[S]) => Option[S],
    numPartitions: Int
  ): DStream[(K, S)] = ssc.withScope {
  updateStateByKey(updateFunc, defaultPartitioner(numPartitions))
}
3、传入更新函数和自定义分区
def updateStateByKey[S: ClassTag](
    updateFunc: (Seq[V], Option[S]) => Option[S],
    partitioner: Partitioner
  ): DStream[(K, S)] = ssc.withScope {
  val cleanedUpdateF = sparkContext.clean(updateFunc)
  val newUpdateFunc = (iterator: Iterator[(K, Seq[V], Option[S])]) => {
    iterator.flatMap(t => cleanedUpdateF(t._2, t._3).map(s => (t._1, s)))
  }
  updateStateByKey(newUpdateFunc, partitioner, true)
}
4、传入完整的状态更新函数
前面的函数传入的都是不完整的更新函数，只是针对一个key的，他们在执行的时候也会生成一个完整的状态更新函数。
Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)] 入参是一个迭代器，参数1是key，参数2是这个key在这个batch中更新的值的集合，参数3是当前状态，最终得到key-->newvalue
def updateStateByKey[S: ClassTag](
    updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
    partitioner: Partitioner,
    rememberPartitioner: Boolean
  ): DStream[(K, S)] = ssc.withScope {
   new StateDStream(self, ssc.sc.clean(updateFunc), partitioner, rememberPartitioner, None)
}
例如，对于wordcount：

val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
  iterator.flatMap(t => function1(t._2, t._3).map(s => (t._1, s)))
}

5、加入初始状态
 initialRDD: RDD[(K, S)] 初始状态集合
def updateStateByKey[S: ClassTag](
    updateFunc: (Seq[V], Option[S]) => Option[S],
    partitioner: Partitioner,
    initialRDD: RDD[(K, S)]
  ): DStream[(K, S)] = ssc.withScope {
  val cleanedUpdateF = sparkContext.clean(updateFunc)
  val newUpdateFunc = (iterator: Iterator[(K, Seq[V], Option[S])]) => {
    iterator.flatMap(t => cleanedUpdateF(t._2, t._3).map(s => (t._1, s)))
  }
  updateStateByKey(newUpdateFunc, partitioner, true, initialRDD)
}
6、是否记得当前的分区
def updateStateByKey[S: ClassTag](
    updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
    partitioner: Partitioner,
    rememberPartitioner: Boolean,
    initialRDD: RDD[(K, S)]
  ): DStream[(K, S)] = ssc.withScope {
   new StateDStream(self, ssc.sc.clean(updateFunc), partitioner,
     rememberPartitioner, Some(initialRDD))
}

完整的例子：
def testUpdate={
    val sc = SparkUtils.getSpark("test", "db01").sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("hdfs://ns1/config/checkpoint")
    val initialRDD = sc.parallelize(List(("hello", 1), ("world", 1)))
    val lines = ssc.fileStream[LongWritable,Text,TextInputFormat]("hdfs://ns1/config/data/")
    val words = lines.flatMap(x=>x._2.toString.split(","))
    val wordDstream :DStream[(String, Int)]= words.map(x => (x, 1))
    val result=wordDstream.reduceByKey(_ + _)

    def function1(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
      val newCount = newValues.sum + runningCount.getOrElse(0) // add the new values with the previous running count to get the new count
      Some(newCount)
    }
    val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
      iterator.flatMap(t => function1(t._2, t._3).map(s => (t._1, s)))
    }
    val stateDS=result.updateStateByKey(newUpdateFunc,new HashPartitioner (sc.defaultParallelism),true,initialRDD)
    stateDS.print()
    ssc.start()
    ssc.awaitTermination()
  }
————————————————
版权声明：本文为CSDN博主「lmb633」的原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/lmb09122508/article/details/80537881
 */