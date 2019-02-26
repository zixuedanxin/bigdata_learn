package com.yanxml.spark.orginal;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * The Word Count To Test The Performance.
 * */
public class SparkWordCountDemo {
	
	public static void main(String []args) {
//		String logFile = "YOUR_SPARK_HOME/README.md"; // Should be some file on your system
		
		String logFile = "/tmp/word.txt";
		
		// spark
		
		// config the spark config
//		boolean flagLocal = true;
		boolean flagLocal = false;

		SparkConf conf = new SparkConf().setAppName("Simple Application");
		
		if(flagLocal){
			String master ="local[4]";
			logFile = "/Users/Sean/Documents/Gitrep/bigdata/spark-streaming/data/word.txt";
			conf.setMaster(master);
		}
		
				
		SparkContext sparkContext = new SparkContext (conf);
		
//		sparkContext.textFile(path, minPartitions)
		
//		JavaSparkContext sc = new JavaSparkContext(conf);

		SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
		
//	    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
		
		int i =0;
		
		Dataset<String> logData = spark.read().textFile(logFile).cache();
		
		while(i<100){

//		    long numAs = logData.filter(s -> s.contains("a")).count();
//		    long numBs = logData.filter(s -> s.contains("b")).count();
//
//		    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
		    i++;
		}
	    
	    spark.stop();
	}

}
