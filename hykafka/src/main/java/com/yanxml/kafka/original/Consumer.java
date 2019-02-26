package com.yanxml.kafka.original;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import kafka.utils.ShutdownableThread;

public class Consumer extends ShutdownableThread{
	private final KafkaConsumer<Integer, String> consumer;
	private final String topic;
	
	public Consumer(String topic){
		super("KafkaConsumerExample", false);//shutdownableThread 的super函数
		Properties props=new Properties();
	//	props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.100.52:9092");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		
		consumer=new KafkaConsumer<>(props);
		this.topic=topic;		
	}

	//必须需要实现的接口方法
	@Override
	public void doWork() {
		// singletonList方法 在方法调用返回一个只包含指定对象的不可变列表。
		consumer.subscribe(Collections.singletonList(this.topic));//订阅方法
		ConsumerRecords<Integer, String>records=consumer.poll(1000);
		// time代表轮询等待时间
		// poll Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. 
		// It is an error to not have subscribed to any topics or partitions before polling for data.
		// poll是从订阅的topic内拉去数据的方法
		for(ConsumerRecord<Integer, String>record:records){
			System.out.println("Received Message:( "+record.key()+","+record.value()+") at offset "+record.offset());
		}
		
	}

	@Override
	public String name() {
		return null;
	}
	
	//是否可以中断
	@Override
	public boolean isInterruptible() {
		return false;
	}
}
