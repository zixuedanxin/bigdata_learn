package com.yanxml.kafka.newquickstart;

public class RunProducerDemo {

	public static void main(String []args){
//		KafkaConsumerDemo consumerDemo = new KafkaConsumerDemo();
		KafkaProducerDemo producerDemo = new KafkaProducerDemo();
		
		if(null != producerDemo.producer){
			int i=0;
			while(i<100000){
			try{
				producerDemo.send("sean-security", "Hello");i++;
//			producerDemo.send("test5", "Hello");i++;
			Thread.sleep(1000);}catch(Exception e){
				System.out.println("error");
			}
			}
		}
	}
}
