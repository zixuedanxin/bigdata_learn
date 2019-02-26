package com.yanxml.spark.streaming.quickstart.consts;

import java.util.Arrays;
import java.util.Collection;

public class SparkConsts {
	
	// 是否为本地模式运行
	public final static boolean isLocalFlag = true;
	
	// 每次拉取数据的间隔时间
	public final static int sparkBatchDurationMilliseconds = 5000;

	public final static String kafkaBrokerUrl = "localhost:9092,localhost:9093";
	
	public final static Collection<String>  kafkaTopicSet = Arrays.asList("spark-stremingA", "spark-stremingB"); ;

	public final static int sparkRepartitionNum = 24 ;


}
