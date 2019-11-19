package sparkml

import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object logicTest {

  def main(args: Array[String]): Unit = {

    // 0.构建 Spark 对象
    val spark = SparkSession
      .builder()
      .master("local") // 本地测试，否则报错 A master URL must be set in your configuration at org.apache.spark.SparkContext.
      .appName("test")
      .enableHiveSupport()
      .getOrCreate() // 有就获取无则创建

    spark.sparkContext.setCheckpointDir("C:\\LLLLLLLLLLLLLLLLLLL\\BigData_AI\\sparkmlTest") //设置文件读取、存储的目录，HDFS最佳
    import spark.implicits._

    //1 训练样本准备
    val training = spark.createDataFrame(Seq(
      (1.0, Vectors.sparse(692, Array(10, 20, 30), Array(-1.0, 1.5, 1.3))),
      (0.0, Vectors.sparse(692, Array(45, 175, 500), Array(-1.0, 1.5, 1.3))),
      (1.0, Vectors.sparse(692, Array(100, 200, 300), Array(-1.0, 1.5, 1.3))))).toDF("label", "sparkml/features")
    training.show(false)

    /**
      * +-----+----------------------------------+
      * |label|features                          |
      * +-----+----------------------------------+
      * |1.0  |(692,[10,20,30],[-1.0,1.5,1.3])   |
      * |0.0  |(692,[45,175,500],[-1.0,1.5,1.3]) |
      * |1.0  |(692,[100,200,300],[-1.0,1.5,1.3])|
      * +-----+----------------------------------+
      */

    //2 建立逻辑回归模型
    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

    //2 根据训练样本进行模型训练
    val lrModel = lr.fit(training)

    //2 打印模型信息
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    /**
      * Coefficients: (692,[45,175,500],[0.48944928041408226,-0.32629952027605463,-0.37649944647237077]) Intercept: 1.251662793530725
      */

    println(s"Intercept: ${lrModel.intercept}")

    /**
      * Intercept: 1.251662793530725
      */

    //3 建立多元回归模型
    val mlr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setFamily("multinomial")

    //3 根据训练样本进行模型训练
    val mlrModel = mlr.fit(training)

    //3 打印模型信息
    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")

    /**
      * Multinomial coefficients: 0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0  ... (692 total)
      * 0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0  0.0  ...
      */

    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

    /**
      * Multinomial intercepts: [-0.6449310568167714,0.6449310568167714]
      */

    //4 测试样本
    val test = spark.createDataFrame(Seq(
      (1.0, Vectors.sparse(692, Array(10, 20, 30), Array(-1.0, 1.5, 1.3))),
      (0.0, Vectors.sparse(692, Array(45, 175, 500), Array(-1.0, 1.5, 1.3))),
      (1.0, Vectors.sparse(692, Array(100, 200, 300), Array(-1.0, 1.5, 1.3))))).toDF("label", "sparkml/features")
    test.show(false)

    /**
      * +-----+----------------------------------+
      * |label|features                          |
      * +-----+----------------------------------+
      * |1.0  |(692,[10,20,30],[-1.0,1.5,1.3])   |
      * |0.0  |(692,[45,175,500],[-1.0,1.5,1.3]) |
      * |1.0  |(692,[100,200,300],[-1.0,1.5,1.3])|
      * +-----+----------------------------------+
      */

    //5 对模型进行测试
    val test_predict = lrModel.transform(test)
    test_predict
      .select("label", "prediction", "probability", "sparkml/features")
      .show(false)

    /**
      * +-----+----------+----------------------------------------+----------------------------------+
      * |label|prediction|probability                             |features                          |
      * +-----+----------+----------------------------------------+----------------------------------+
      * |1.0  |1.0       |[0.22241243403014824,0.7775875659698517]|(692,[10,20,30],[-1.0,1.5,1.3])   |
      * |0.0  |0.0       |[0.5539602964649871,0.44603970353501293]|(692,[45,175,500],[-1.0,1.5,1.3]) |
      * |1.0  |1.0       |[0.22241243403014824,0.7775875659698517]|(692,[100,200,300],[-1.0,1.5,1.3])|
      * +-----+----------+----------------------------------------+----------------------------------+
      */

    //6 模型摘要
    val trainingSummary = lrModel.summary

    //6 每次迭代目标值
    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss => println(loss))

    /**
      * objectiveHistory:
      * 0.6365141682948128
      * 0.6212055977633174
      * 0.5894552698389314
      * 0.5844805633573479
      * 0.5761098112571359
      * 0.575517297029231
      * 0.5754098875805627
      * 0.5752562156795122
      * 0.5752506337221737
      * 0.5752406742715199
      * 0.5752404945106846
      */

    //6 计算模型指标数据
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    //6 AUC指标
    val roc = binarySummary.roc
    roc.show(false)

    /**
      * +---+---+
      * |FPR|TPR|
      * +---+---+
      * |0.0|0.0|
      * |0.0|1.0|
      * |1.0|1.0|
      * |1.0|1.0|
      * +---+---+
      */

    val AUC = binarySummary.areaUnderROC
    println(s"areaUnderROC: ${binarySummary.areaUnderROC}")

    //6 设置模型阈值
    //不同的阈值，计算不同的F1，然后通过最大的F1找出并重设模型的最佳阈值。
    val fMeasure = binarySummary.fMeasureByThreshold
    fMeasure.show(false)

    /**
      * +-------------------+---------+
      * |threshold          |F-Measure|
      * +-------------------+---------+
      * |0.7775875659698517 |1.0      |
      * |0.44603970353501293|0.8      |
      * +-------------------+---------+
      */

    //获得最大的F1值
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    //找出最大F1值对应的阈值（最佳阈值）
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure).select("threshold").head().getDouble(0)
    //并将模型的Threshold设置为选择出来的最佳分类阈值
    lrModel.setThreshold(bestThreshold)

    //7 模型保存与加载（发布到服务器 django 时，View 加入如下代码 + 文件）
//    lrModel.save("C:\\LLLLLLLLLLLLLLLLLLL\\BigData_AI\\sparkmlTest\\lrmodel")
//    val load_lrModel = LogisticRegressionModel.load("C:\\LLLLLLLLLLLLLLLLLLL\\BigData_AI\\sparkmlTest\\lrmodel")

  }

}
