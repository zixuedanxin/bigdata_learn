package opensource
import scala.reflect.ClassTag

/*
     *  泛型[]，中括号F、S、T都表示运行时参数类型，
     * ClassTag[T]保存了泛型擦除后的原始类型T,提供给被运行时的。
     */

class Triple[F: ClassTag, S, T](val first: F, val second: S, val third: T)


object MainTest{
  def main(args: Array[String]): Unit = {
    // 运行执行代码：val triple: Triple[String, Int, Double]
    val triple = new Triple("Spark", 3, 3.1415)
    // 运行执行代码：val bigData: Triple[String, String, Char]
    val bigData = new Triple[String, String, Char]("Spark", "Hadoop", 'R');
    println(bigData)
    // getData函数传入泛型为T的运行时List类型参数，返回list.length / 2的整数。
    def getData[T](list:List[T]) = list(list.length / 2)
    // List索引从0开始，执行结果：Hadoop
    println(getData(List("Spark","Hadoop",'R')));
    // 获得getData函数引用
    val f = getData[Int]_
    // 调用getData函数，执行结果：4
    println(f(List(1,2,3,4,5,6)));
    def mkArray[T:ClassTag](elems:T*) = Array[T](elems:_*)

    mkArray(42,13).foreach(println)

    mkArray("Japan","Brazil","Germany").foreach(println)
  }
}
