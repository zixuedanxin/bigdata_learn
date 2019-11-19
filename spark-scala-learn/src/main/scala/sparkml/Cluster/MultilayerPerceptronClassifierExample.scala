package sparkml.Cluster

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

object MultilayerPerceptronClassifierExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MultilayerPerceptronClassifierExample")
      .master("local")
      .getOrCreate()

    val data = spark.read.format("libsvm").load("data/mllib/sample_multiclass_classification_data.txt")

    val splits = data.randomSplit(Array(0.6, 0.4), seed = 1234L)
    val train = splits(0)
    val test = splits(1)
    //    设置神经网络的层数
    //    输入层有4个神经元、两个隐含层分别有5和4个神经元，输出层有3个
    val layers = Array[Int](4, 5, 4, 3)

    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)

    val model = trainer.fit(train)

    val result = model.transform(test)
    val predictionAndLabel = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
    println("Test set accuracy = " + evaluator.evaluate(predictionAndLabel))
  }
}
