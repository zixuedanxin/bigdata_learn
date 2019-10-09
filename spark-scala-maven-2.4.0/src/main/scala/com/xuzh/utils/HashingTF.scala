package com.xuzh.utils

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{BooleanParam, IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.feature
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class HashingTF(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  private[this] var hashingTF: feature.HashingTF = _

  def this() = this(Identifiable.randomUID("hashingTF"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
    * Number of features. Should be greater than 0.
    * (default = 2^18^)
    * @group param
    */
  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)",
    ParamValidators.gt(0))

  /**
    * Binary toggle to control term frequency counts.
    * If true, all non-zero counts are set to 1.  This is useful for discrete probabilistic
    * models that model binary events rather than integer counts.
    * (default = false)
    * @group param
    */
  val binary = new BooleanParam(this, "binary", "If true, all non zero counts are set to 1. " +
    "This is useful for discrete probabilistic models that model binary events rather " +
    "than integer counts")

  setDefault(numFeatures -> (1 << 18), binary -> false)

  /** @group getParam */
  def getNumFeatures: Int = $(numFeatures)

  /** @group setParam */
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  /** @group getParam */
  def getBinary: Boolean = $(binary)

  /** @group setParam */
  def setBinary(value: Boolean): this.type = set(binary, value)

  def indexOf(term: Any): Int = {
    if (hashingTF != null) {
      hashingTF.indexOf(term)
    } else {
      throw UninitializedFieldError("Use transform method to initialize the model at first.")
    }
  }


  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    hashingTF = new feature.HashingTF($(numFeatures)).setBinary($(binary))
    // TODO: Make the hashingTF.transform natively in ml framework to avoid extra conversion.
    val t = udf { terms: Seq[_] => hashingTF.transform(terms).asML }
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[ArrayType],
      s"The input column must be ArrayType, but got $inputType.")
    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }



  override def copy(extra: ParamMap): HashingTF = defaultCopy(extra)
}

object HashingTF extends DefaultParamsReadable[HashingTF] {

  override def load(path: String): HashingTF = super.load(path)
}
