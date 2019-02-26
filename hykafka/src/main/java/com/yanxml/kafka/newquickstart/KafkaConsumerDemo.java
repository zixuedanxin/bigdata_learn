package com.yanxml.kafka.newquickstart;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerDemo {
	public KafkaConsumer<String, String> consumer;

	public KafkaConsumerDemo() {
		Properties props = new Properties();
		System.setProperty("java.security.auth.login.config",
				"/Users/Sean/Documents/Gitrep/bigdata/kafka/src/main/resources/kafka_client_jaas.conf"); // 环境变量添加，需要输入配置文件的路径
		props.put("security.protocol", "SASL_PLAINTEXT");
		props.put("sasl.mechanism", "PLAIN");
		props.put("bootstrap.servers", KafkaConfig.kafkaUrl);
//		props.put("group.id", "console-consumer-44277");

		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(KafkaConfig.topic));// 订阅的Topic 可以写多个
																// "TopicA","TopicB","TopicC"
	}

	public void consume() {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records){
				System.out.printf("offset = %d, key = %s, value = %s%n",
						record.offset(), record.key(), record.value());
			}
				
			
			
		}
	}

}
