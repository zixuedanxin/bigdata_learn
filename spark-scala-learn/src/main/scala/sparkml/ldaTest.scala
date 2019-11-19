package sparkml


import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.sql.SparkSession

object ldaTest {

  def main(args: Array[String]): Unit = {

    // 0.构建 Spark 对象
    val spark = SparkSession
      .builder()
      .master("local") // 本地测试，否则报错 A master URL must be set in your configuration at org.apache.spark.SparkContext.
      .appName("test")
      .enableHiveSupport()
      .getOrCreate() // 有就获取无则创建

    // 1.读取样本
    val dataset = spark.read.format("libsvm").load("C:\\LLLLLLLLLLLLLLLLLLL\\BigData_AI\\data.txt")
    dataset.show()

    // 2.训练 LDA model.
    val lda = new LDA().setK(10).setMaxIter(10)
    val model = lda.fit(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")

    // 3.主题 topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    val aa = model.topicsMatrix
    model.estimatedDocConcentration
    model.getTopicConcentration

    // 4.测试结果.
    val transformed = model.transform(dataset)
    transformed.show(false)
    transformed.columns

    // 5.模型保存与加载
//    model.save("C:\\LLLLLLLLLLLLLLLLLLL\\BigData_AI\\ldamodel")

    spark.stop()

  }

}
