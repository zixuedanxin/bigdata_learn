package sparkml.rdd

//import org.apache.spark.ml.classification.LogisticRegression
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}

object EstimatorTransformerParamExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EstimatorTransformerParamExample")
      .master("local")
      .getOrCreate()
    //创建训练集
    val training = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")

    //创建逻辑回归实例，这是一个模型学习器
    val lr = new LogisticRegression()
    //打印默认参数
//    println("LogisticRegression parameters:\n" + lr.explainParam() + "\n")

    //设置自定义参数
    lr.setMaxIter(10)
      .setRegParam(0.01)

    //用默认参数学习一个模型
    val model1 = lr.fit(training)
    //model1是一个由模型学习器产生的转换器，我们可以看到它在ｆｉｔ时用的参数
    print("Model 1 was fit using parameters:" + model1.parent.extractParamMap)

    //可以使用ParaMap指定参数
    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter, 30) //将会覆盖之前的maxIter
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55) //指定多个参数

    //可以合并两个ParamMap
    val paramMap2 = ParamMap(lr.probabilityCol -> "myPro")
    //改变输出列名
    val paramMapCombined = paramMap ++ paramMap2

    val model2 = lr.fit(training, paramMapCombined)
    print("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

    //构造测试集
    val test = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5))
    )).toDF("label", "features")

    model2.transform(test)
      .select("features", "label", "myPro", "prediction")
      .collect()
      .foreach {
        case Row(features, label: Double, prob, prediction: Double) =>
        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
      }
  }
}
