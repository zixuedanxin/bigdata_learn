package sparkml.Kmeans

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KMeansClustering {
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage:KMeansClustering trainingDataFilePath numClusters numIterations")
      sys.exit(1)
    }

    //??????????????
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //??????
    val conf = new SparkConf().setAppName("KMeansClusting")
    val sc = new SparkContext(conf)

    //?????
    val data = sc.textFile(args(0))
    val parsedTrainingData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))

    //???????????????????
    val numClusters = args(1).toInt
    val numIterations = args(2).toInt
    val model = KMeans.train(parsedTrainingData, numClusters, numIterations)

    //??????????
    println("Cluster centers:")
    for (c <- model.clusterCenters) {
      println("  " + c.toString)
    }

    //???????????????
    val ks:Array[Int] = Array(1,2,3,4,5,6,7,8,9,10)
    ks.foreach(cluster => {
      val model:KMeansModel = KMeans.train(parsedTrainingData, cluster,30)
      val ssd = model.computeCost(parsedTrainingData)
      println("Within Set Sum of Squared Errors When K=" + cluster + " -> "+ ssd)
    })

    //??????????
    println("Vectors 2.0 2.0 2.0 is belongs to clusters:" + model.predict(Vectors.dense("2.0 2.0 2.0".split(' ').map(_.toDouble))))
    println("Vectors 20 20 20 is belongs to clusters:" + model.predict(Vectors.dense("20 20 20".split(' ').map(_.toDouble))))
    println("Vectors 66.2 66.2 66.2 is belongs to clusters:" + model.predict(Vectors.dense("66.2 66.2 66.2".split(' ').map(_.toDouble))))

    //?????????????
    val result = data.map {
      line =>
        val linevectore = Vectors.dense(line.split(' ').map(_.toDouble))
        val prediction = model.predict(linevectore)
        line + " " + prediction
    }
    result.collect().foreach(line => println(line))

    sc.stop()
  }
}
