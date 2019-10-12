package sljr.ybd.spark.test.seri
import org.apache.spark.SparkContext

object Spark {
  val ctx = new SparkContext("local", "test")
}
object NOTworking extends App {
  new Test().doIT
}
class Test extends java.io.Serializable {
  val rddList = Spark.ctx.parallelize(List(1,2,3))

  def doIT() =  {
    val after = rddList.map(someFunc)
    after.collect().foreach(println)
  }

  def someFunc(a: Int) = a + 1
}