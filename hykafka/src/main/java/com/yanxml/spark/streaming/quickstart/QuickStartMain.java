package com.yanxml.spark.streaming.quickstart;

import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.yanxml.spark.streaming.quickstart.conf.SparkConfig;

public class QuickStartMain {
	
	// 开始JavaContext
	public static boolean start() {
		JavaStreamingContext javaStreamingContext = SparkConfig
				.getJavaStreamingContext();
		javaStreamingContext.start();
		try {
			javaStreamingContext.awaitTermination();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	public static void main(String[] args) {
		start();
	}

}
