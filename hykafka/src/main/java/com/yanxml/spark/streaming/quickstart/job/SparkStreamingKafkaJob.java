package com.yanxml.spark.streaming.quickstart.job;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import com.yanxml.spark.streaming.quickstart.conf.SparkConfig;
import com.yanxml.spark.streaming.quickstart.consts.SparkConsts;

// 根据初始化变量 初始化Spark Streaming数据流
public class SparkStreamingKafkaJob {
	
	// 本初始流程主要包括三个
	public SparkStreamingKafkaJob(){
		JavaDStream<String> kafkaDataStream = initKafkaDataStream();
		// 对原始事件进行处理
		JavaDStream<Optional<Integer>> validateResultDataStream = volidateStreamData(kafkaDataStream);
		JavaDStream<Optional<String>> concateResultDataStream = concatStreamData(validateResultDataStream);
		endStreamData(concateResultDataStream);
	}
	
	// 初始化Kafka数据流 从Kafka里读取String类型数据流
	public JavaDStream<String> initKafkaDataStream(){
		JavaDStream<String> result = null;
		Map<String,Object> kafkaParamMap = new HashMap<>();
		kafkaParamMap.put("bootstrap.servers", SparkConsts.kafkaBrokerUrl);
		kafkaParamMap.put("key.deserializer", StringDeserializer.class);
		kafkaParamMap.put("value.deserializer", StringDeserializer.class);
		kafkaParamMap.put("group.id", "sean_kafka_consumer_group");
		kafkaParamMap.put("auto.offset.reset", "latest");//每次拉取最后的数据
		kafkaParamMap.put("enable.auto.commit", true);//自动提交
		
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
				SparkConfig.getJavaStreamingContext(), LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(SparkConsts.kafkaTopicSet, kafkaParamMap));
		
		result = stream.map(pair -> {
			return pair.value();
		});
		
		// 数据重新分片
		if(SparkConsts.sparkRepartitionNum > 1){
			result.repartition(SparkConsts.sparkRepartitionNum);
		}
		
		// 返回 JavaDStream<String>类型的数据流
		return result;
	}
	
	// 数据流第一步处理
	// 注意参数类型和返回值类型的改变是如何实现的。
	public JavaDStream<Optional<Integer>> volidateStreamData(JavaDStream<String> javaDStream){
		JavaDStream<Optional<Integer>> resultJavaDStream = javaDStream.map(data->{
			// 处理逻辑可以自己写的复杂一些
//			data = data.concat("1");
			int i=1;
			return Optional.of(i);
		});
		return resultJavaDStream;
	}
	
	public JavaDStream<Optional<String>> concatStreamData(JavaDStream<Optional<Integer>> javaDStream){
		return javaDStream.filter(e->e.isPresent())
				.map(e->e.get()).map(data->{
					return Optional.of("hello");
				});
	}
	
	public boolean endStreamData(JavaDStream<Optional<String>> javaDStream){
		javaDStream.filter(e->e.isPresent())
		.map(e->e.get())
		.foreachRDD(rdd->{
			rdd.foreach(data->{
//				log.info(data);
				// do something
			});
		});
    	return true;
	}
}
