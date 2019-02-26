package com.yanxml.spark.streaming.quickstart.conf;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.yanxml.spark.streaming.quickstart.consts.SparkConsts;


/*
 * spark streaming 初始类
 * word count
 * */
public class SparkConfig {

	private static SparkContext sparkContext;
	private static JavaSparkContext javaSparkContext;
	private static JavaStreamingContext javaStreamingContext;
	// SparkSession sparkSession;

	static {
		SparkConf sparkConf = new SparkConf().setAppName("NetworkWordCount");
		sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
		if (SparkConsts.isLocalFlag) {
			sparkConf.setMaster("local[2]");
			sparkConf.set("spark.driver.bindAddress", "127.0.0.1");
		}

		sparkContext = new SparkContext(sparkConf);
		javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
		javaStreamingContext = new JavaStreamingContext(javaSparkContext,
				new Duration(SparkConsts.sparkBatchDurationMilliseconds));
	}

	public static SparkContext getSparkContext() {
		return sparkContext;
	}
	
	public static JavaSparkContext getJavaSparkContext() {
		return javaSparkContext;
	}
	
	public static JavaStreamingContext getJavaStreamingContext() {
		return javaStreamingContext;
	}
	
	public static SparkSession getSparkSession() {
		return SparkSession.builder().sparkContext(sparkContext).getOrCreate();
	}

}
