package com.yanxml.kafka.newquickstart;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.yanxml.kafka.newquickstart.callback.MyCallBack;


public class KafkaProducerDemo {

	public static Producer<String, String> producer;

	public KafkaProducerDemo() {
		Properties props = new Properties();
		System.setProperty("java.security.auth.login.config",
				"/Users/Sean/Documents/Gitrep/bigdata/kafka/src/main/resources/kafka_client_jaas.conf"); // 环境变量添加，需要输入配置文件的路径
		props.put("security.protocol", "SASL_PLAINTEXT");
		props.put("sasl.mechanism", "PLAIN");
		props.put("bootstrap.servers", KafkaConfig.kafkaUrl);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<>(props);
		
		// 让producer connect
		
	}

	public boolean send(String topicName, String content) {
		
		
		try {
			Future<RecordMetadata> result= producer.send(new ProducerRecord<String, String>(topicName, content),new MyCallBack(topicName,content,KafkaConfig.kafkaUrl));
//			System.out.println("sum():"+ result.get().checksum());
//			System.out.println("sum():"+ result.get().hashCode());
//			System.out.println("sum():"+ result.get().offset());
//			System.out.println("sum():"+ result.get().hasOffset());
//			Thread.sleep(1000);
			return true;
		} catch (Exception e) {
			System.out.println("error");
			return false;
		}
		
//		if(null != producer){
//			producer.send(new ProducerRecord<String, String>("sean-security",
//					"HELLO"));
////			for (int i = 0; i < 100; i++){
////				producer.send(new ProducerRecord<String, String>("sean-security",
////						"HELLO"+i));
////			}
//			// producer.close();
//
//		}else{
//			System.out.println("NullPointer");
//		}

	}

}
