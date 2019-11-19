package sparkml.LinearRegression

import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.sql.SparkSession

object GeneralizedLinearRegressionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("GeneralizedLinearRegressionExample")
      .getOrCreate()

    val dataset = spark.read.format("libsvm")
      .load("data/mllib/sample_linear_regression_data.txt")

    val glr = new GeneralizedLinearRegression()
      .setFamily("gaussian")//模型中使用的误差分布类型。
      .setLink("identity")//连接函数名，描述线性预测器和分布函数均值之间关系
      .setMaxIter(10)
      .setRegParam(0.3)

    val  model = glr.fit(dataset)
//  输出权值向量和截距
    println(s"Coefficients: ${model.coefficients}")
    println(s"Intercept: ${model.intercept}")

    val summary = model.summary
    println(s"Coefficient Standart errors: ${summary.coefficientStandardErrors.mkString(", ")}")
    println(s"T Values: ${summary.tValues.mkString(",")}")
    println(s"P Values: ${summary.pValues.mkString(",")}")
    println(s"Dispersion: ${summary.dispersion}")
    println(s"Null Deviance: ${summary.nullDeviance}")
    println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
    println(s"Deviance: ${summary.deviance}")
    println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
    println(s"AIC: ${summary.aic}")
    println("Deviance Residuals: ")
    summary.residuals().show()
  }
}
