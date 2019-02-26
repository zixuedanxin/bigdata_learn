package com.yanxml.kafka.quickstart;

/**
 * 
 * @author yangyibo
 *
 */
public class Client {
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
//		KafkaProducerDemo.creatKafkaProducer(KafkaConfig.Producer_Topic);
		KafkaOldProducer.creatProducer("sean");
//		KafkaConsumer.creatConsumer(KafkaConfig.Consumer__Topic);
	}

}
