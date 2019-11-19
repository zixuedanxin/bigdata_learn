package sparkml.rdd

import org.apache.spark.mllib.linalg.Vectors


object testVector {
  def main(args: Array[String]): Unit = {
    val vd   = Vectors.dense(1,0,6)
    println(vd(2))
    println(vd)
    val vs = Vectors.sparse(6, Array(0,1,3,5), Array(9,5,2,7))
    println(vs(5))
    println(vs)
  }
}
