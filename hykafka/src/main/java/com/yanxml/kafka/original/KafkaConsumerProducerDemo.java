package com.yanxml.kafka.original;

public class KafkaConsumerProducerDemo {
	public static void main(String []args){
//		Boolean isAsync=(args.length==0||args[0].trim().equals("isAsync"));//默认是异步发布
//		Producer poducerThread=new Producer(KafkaProPerties.TOPIC, isAsync,"DemoProducer");
//		poducerThread.start();
//		
//		Consumer consumerThread=new Consumer(KafkaProPerties.TOPIC);
//		consumerThread.start();
		
		Consumer consumerThread=new Consumer("test");
		consumerThread.start();


	}

}
