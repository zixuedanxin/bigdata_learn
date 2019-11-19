package sparkml.LinearRegression

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object LinearRegressionWithElasticNetExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("OneVsRestExample")
      .getOrCreate()

    val training = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val lr= new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lrModel = lr.fit(training)
    println(s"Coefficients : ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary
    println(s"num iterations : ${trainingSummary.totalIterations}")
    println(s"objectiveHistory : ${trainingSummary.objectiveHistory.mkString(", ")}")
    trainingSummary.residuals.show()//残差
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")//均方误差
    println(s"r2: ${trainingSummary.r2}")//决定系数
  }
}
