package sljr.ybd.spark.test
import org.apache.spark.SparkContext
import org.apache.spark._
object NOTworking extends App {
  val list = List(1,2,3)
  val ctx = new SparkContext("local", "test")
  val rddList = ctx.parallelize(list)
  //calling function outside closure
  val after = rddList.map(someFunc(_))
  def someFunc(a:Int)  = a+1
  after.collect().map(println(_))
  //new testing().doIT
}
//adding extends Serializable wont help
class testing {
  val ctx = new SparkContext("local", "test")
  val list = List(1,2,3)

  val rddList = ctx.parallelize(list)

  def doIT =  {
    //again calling the fucntion someFunc
    val after = rddList.map(someFunc(_))
    //this will crash (spark lazy)
    after.collect().map(println(_))
  }

  def someFunc(a:Int) = a+1

}