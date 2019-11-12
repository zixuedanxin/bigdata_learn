package rdd

import org.apache.spark.sql.SparkSession

import scala.util.Random

object RddExamples extends App {

  val spark = SparkSession.builder().appName("rdd-example").master("local[*]").getOrCreate()

  //creating an rdd. Directly using rdd method on dataframe/dataset
  val rdd = spark.range(10).toDF().rdd

  println(rdd.map(_.getLong(0)))

  //another way converting collection to rdd using parallelize method
  val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
  val words = spark.sparkContext.parallelize(myCollection, 2)

  //creating a key/value pair rdd
  words.map(word => (word.toLowerCase, 1)).foreach(println)

  //another way to create key/value pair using keyBy method
  val pairRdd = words.keyBy(word => word.toLowerCase.toSeq.head.toString)
  pairRdd.foreach(println)

  //mapping over values
  pairRdd.mapValues(word => word.toUpperCase).collect().foreach(println)

  //Extracting keys and values
  pairRdd.keys.foreach(println)
  pairRdd.values.foreach(println)

  //finding particular key using lookup method
  pairRdd.lookup("s").foreach(println)

  //aggregation
  val chars = words.flatMap(word => word.toLowerCase.toSeq)
  val keyValueChars = chars.map(letter => (letter, 1))

  def maxFunc(left: Int, right: Int) = math.max(left, right)
  def addFunc(left: Int, right: Int) = left +  right
  val nums = spark.sparkContext.parallelize(1 to 30, 5)

  nums.foreach(println)

  //finding number of elements in particular key using countByKey
  val timeout = 1000L
  val confidence = 0.95

  println("countByKey result: " + keyValueChars.countByKey())
  println("countByKeyApproax result: " + keyValueChars.countByKeyApprox(timeout, confidence))

  //groupByKey
  keyValueChars.groupByKey()
    .map(row => (row._1, row._2.reduce(addFunc)))
    .collect().foreach(println)

  /*
  There is problem with above approach. If dataset is large, it is difficult to scale and give OOM error.
  Preferred way to use reduceByKey. It will first apply reduce function on every worker and the get the data in memory and apply final reduce.
  Order will not be same. This operation is fine if order does not matter.
   */

  keyValueChars.reduceByKey(addFunc).collect().foreach(println)

  /* aggregate function: It takes an initial value and two functions. First function applies to data in every partition, second
  function to combine the data of different partition. Combine functions happens in driver if data is to large, it might cause OOM
  error. Better to use treeAggregate in such cases.
  * */

  println("aggregate response " + nums.aggregate(0)(maxFunc, addFunc))

  /*
    treeAggregate: It works same way as aggregate but in a different way. It basically pushed down some of the aggregations
    (creating a tree from executor to executor) before performing the final aggregation on the driver. Having multiple levels
    can help you to ensure that driver program does not return out of memory in the process of aggregation.
  */
   println("treeAggregate response " + nums.treeAggregate(0)(maxFunc, addFunc))

  //aggregateByKey: Does same as aggregate but instead of doing it partition by partition, it does it by key.
  println("aggregateByKey response: " + keyValueChars.aggregateByKey(0)(addFunc, maxFunc).collect().toList)

  /*
  combineByKey: Instead of specifying an aggregation function, you can specify a combiner. This combiner operates on a given key
  and merges the values according to some function. It then goes to merge the different outputs of the combiners to give us our result.
  We can specify the number of output partitions as a custom output paritioner as well.
   */
  val valToCombiner = (value: Int) => List(value)
  val mergeValueFunc = (vals: List[Int], valToAppend: Int) => valToAppend :: vals
  val mergeCombineFunc = (list1: List[Int], list2: List[Int]) => list1 ::: list2
  val outputPartitions = 6

  keyValueChars.combineByKey(
    valToCombiner,
    mergeValueFunc,
    mergeCombineFunc,
    outputPartitions
  ).collect().foreach(println)

  /*
  foldByKey: merges the value for each key using an associative functions and a neutral "zero value" which
  can be added to the result an arbitrary number of times.
   */
  keyValueChars.foldByKey(0)(addFunc).collect().foreach(println)

  /*
  cogroup: Gives an ability to group together up to three key-value RDDs together in scala and 2 in python.
  Group based join on a RDD.
   */
  val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct()
  val charRdd = distinctChars.map(c => (c, Random.nextDouble()))
  val charRdd2 = distinctChars.map(c => (c, Random.nextDouble()))
  val charRdd3 = distinctChars.map(c => (c, Random.nextDouble()))
  println("charRdd2",charRdd.collect())
  println("charRdd2",charRdd.collect())
  charRdd.cogroup(charRdd2, charRdd3).take(5).foreach(println)
  println("cogroup",charRdd.collect())
  /*
  Inner Join
   */

  println("joining result: " + keyValueChars.join(charRdd).count())

  //zip operation: joins two rdd having same length and equal partitions
  val num = spark.sparkContext.parallelize(1 to 10, 2)
  words.zip(num).collect().foreach(println)

  /*
  coalesce: Effectively collapses partition on the same worker in order to avoid a shuffling of the data when repartitioning.
   For example, words rdd has two partitions, we can collapse that to one partition by using coalesce without bringing
    about a shuffle of the data.
   */
  println("coalesce result: " + words.coalesce(1).getNumPartitions)

  /*
  repartition: Allows us to repartition the data up or down but performs a shuffle across nodes in the process.
  Increase the number of partitions can increase the number of parallelisms when operating in map and filter type operations.
   */
  println("repartition result: " + words.repartition(10).getNumPartitions)
}