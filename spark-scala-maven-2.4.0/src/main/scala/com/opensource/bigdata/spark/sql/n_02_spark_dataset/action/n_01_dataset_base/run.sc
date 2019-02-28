
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
  .master("local")
  .appName("SparkSessionWordCount")
//  .config("spark.eventLog.enabled","true")
//  .config("spark.history.fs.logDirectory","hdfs://standalone.com:9000/spark/log/historyEventLog")
//  .config("spark.eventLog.dir","hdfs://standalone.com:9000/spark/log/historyEventLog")
  .getOrCreate()



//spark.sparkContext.addJar("/opt/n_001_workspaces/bigdata/spark-scala-maven-2.4.0/target/spark-scala-maven-2.4.0-1.0-SNAPSHOT.jar")



val dataSet = spark.read.textFile("src/main/resource/data/text/people.txt")
