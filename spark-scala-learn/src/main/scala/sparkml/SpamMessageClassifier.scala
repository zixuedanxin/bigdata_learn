import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, Word2Vec, StringIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession

object SpamMessageClassifier {

  def main(args: Array[String]) {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

//    //对输入参数个数进行判断，如果输入参数不为1则退出
//    if (args.length != 1) {
//      println("Usage: /path/to/spark/bin/spark-submit --master spark://master:9000 " +
//        "--driver-memory 1g --class chapter9.SpamMessageClassifier " +
//        "sparklearning.jar SMSDataFilePath")
//      sys.exit(1)
//    }

    //设置应用程序运行环境
    val conf = new SparkConf().setAppName("SpamMessageClassifier").setMaster("local").setAppName("graph_test")
    val sc = new SparkContext(conf)
       //SparkSession.builder()
    val spark=SparkSession.builder.config(conf).getOrCreate()
    val sqlCtx =spark.sqlContext //new SQLContext(sc)
    //SparkSession.builder.
    // spark.sqlContext
//    val sparkSession = SparkSession.builder
//      .master("master")
//      .appName("appName")
//      .getOrCreate()
    //SparkSession.builder.config(conf=SparkConf())


    //从HDFS读取手机短信作为处理数据源，在此基础上创建DataFrame，该DataFrame包含labelCol、contextCol两个列
    val messageRDD = sc.textFile(args(0)).map(_.split("\t")).map(line => {
      (line(0),line(1).split(" "))
    })
    val smsDF = sqlCtx.createDataFrame(messageRDD).toDF("labelCol", "contextCol")

    //将原始的文本标签 (“Ham”或者“Spam”) 转化成数值型的表型
    val labelIndexer = new StringIndexer()
      .setInputCol("labelCol")
      .setOutputCol("indexedLabelCol")
      .fit(smsDF)

    //使用 Word2Vec 将短信文本转化成数值型词向量
    val word2Vec = new Word2Vec()
      .setInputCol("contextCol")
      .setOutputCol("featuresCol")
      .setVectorSize(100)
      .setMinCount(1)

    val layers = Array[Int](100,6,5,2)

    //使用 MultilayerPerceptronClassifier 训练一个多层感知器模型
    val mpc = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(512)
      .setSeed(1234L)
      .setMaxIter(128)
      .setFeaturesCol("featuresCol")
      .setLabelCol("indexedLabelCol")
      .setPredictionCol("predictionCol")

    //使用 LabelConverter 将预测结果的数值标签转化成原始的文本标签
    val labelConverter = new IndexToString()
      .setInputCol("predictionCol")
      .setOutputCol("predictedLabelCol")
      .setLabels(labelIndexer.labels)

    //将原始文本数据按照 8:2 的比例分成训练和测试数据集
    val Array(trainingData, testData) = smsDF.randomSplit(Array(0.8, 0.2))

    //使用pipeline对数据进行处理和模型的训练
    val pipeline = new Pipeline().setStages(Array(labelIndexer, word2Vec, mpc, labelConverter))
    val model = pipeline.fit(trainingData)
    val preResultDF = model.transform(testData)

    //使用模型对测试数据进行分类处理并在屏幕打印20笔数据
    preResultDF.select("contextCol", "labelCol", "predictedLabelCol").show(20)

    //测试数据集上测试模型的预测精确度
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabelCol")
      .setPredictionCol("predictionCol")
    val predictionAccuracy = evaluator.evaluate(preResultDF)
    println("Testing Accuracy is %2.4f".format(predictionAccuracy * 100) + "%")
    sc.stop
  }
}