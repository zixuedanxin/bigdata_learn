// package com.struct.stream
package org.apache.spark.mllib
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.util.MLUtils

import scala.language.implicitConversions

object StreamingMLUtils {
  implicit def mlToMllibVector(v: Vector): OldVector = v match {
    case dv: DenseVector => OldVectors.dense(dv.toArray)
    case sv: SparseVector => OldVectors.sparse(sv.size, sv.indices, sv.values)
    case _ => throw new IllegalArgumentException
  }

  def fastSquaredDistance(x: Vector, xNorm: Double, y: Vector, yNorm: Double) = {
    MLUtils.fastSquaredDistance(x, xNorm, y, yNorm)
  }
}
