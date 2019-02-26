package com.yanxml.kafka.original;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

public class SimpleConsumerDemo {

	private static void printMessages(ByteBufferMessageSet messageSet)
			throws UnsupportedEncodingException {
		for (MessageAndOffset messageAndOffset : messageSet) {
			ByteBuffer payload = messageAndOffset.message().payload();
			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);
			System.out.println(new String(bytes, "UTF-8"));
		}
	}

	private static void generateDate() {
		Producer producer2 = new Producer(KafkaProPerties.TOPIC2, false,"DemoProducer2");
		producer2.start();
		Producer producer3 = new Producer(KafkaProPerties.TOPIC3, false,"DemoProducer3");
		producer3.start();
		try {
			Thread.sleep(1000);// 1000ms 就是1s
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		generateDate();
		// 注意这边会导致一个异常，因为 producer 这个进程 永远会是开着的，只有等待一定时间之后，进程才会销毁
		// 所以关闭第一次运行程序，再第二次运行的时候会报错 因为有重名的进程正在进行
		// 解决办法 让线程一定时间之后 自动销毁
		

//		SimpleConsumer simpleConsumer = new SimpleConsumer(
//				KafkaProPerties.KAFKA_SERVER_URL,
//				KafkaProPerties.KAFKA_SERVER_PORT,
//				KafkaProPerties.CONNECTION_TIMEOUT,
//				KafkaProPerties.KAFKA_PRODUCER_BUFFER_SIZE,
//				KafkaProPerties.CLIENT_ID);
//		System.out.println("Test single fetch");

//		FetchRequest req = new FetchRequestBuilder()
//				.clientId(KafkaProPerties.CLIENT_ID)
//				.addFetch(KafkaProPerties.TOPIC2, 0, 0L, 100).build();
//
//		FetchResponse fetchResponse = simpleConsumer.fetch(req);
//		printMessages(fetchResponse.messageSet(KafkaProPerties.TOPIC2, 0));

		// System.out.println("Test Single multi-fetch");
		// Map<String,List<Integer>> topicMap=new HashMap<>();
		// topicMap.put(KafkaProPerties.TOPIC2,
		// Collections.singletonList(0));//貌似kafka的map一般都会采用<String,List<Integer>>的形式
		// topicMap.put(KafkaProPerties.TOPIC3,
		// Collections.singletonList(0));//List<Integer>
		// 表示这条消息partition的offset的位置 offset相当于一个标尺 上下来回移动
		// FetchRequest req2=new FetchRequestBuilder()
		// .clientId(KafkaProPerties.CLIENT_ID)
		// .addFetch(KafkaProPerties.TOPIC2, 0,0L,100)
		// .addFetch(KafkaProPerties.TOPIC3, 0, 0L, 100).build();
		// FetchResponse fetchResponse2=simpleConsumer.fetch(req2);
		// int fetchReq=0;
		// for(Map.Entry<String,List<Integer>> entry:topicMap.entrySet()){
		// String topic=entry.getKey();
		// for(Integer offset:entry.getValue()){
		// System.out.println("Response from fetch request no: "+ ++fetchReq);
		// printMessages(fetchResponse2.messageSet(topic, offset));
		// }
		// }
	}

}

//异常
//
		//javax.management.InstanceAlreadyExistsException: kafka.producer:type=app-info,id=DemoProducer
//		at com.sun.jmx.mbeanserver.Repository.addMBean(Repository.java:437)
//		at com.sun.jmx.interceptor.DefaultMBeanServerInterceptor.registerWithRepository(DefaultMBeanServerInterceptor.java:1898)
//		at com.sun.jmx.interceptor.DefaultMBeanServerInterceptor.registerDynamicMBean(DefaultMBeanServerInterceptor.java:966)
//		at com.sun.jmx.interceptor.DefaultMBeanServerInterceptor.registerObject(DefaultMBeanServerInterceptor.java:900)
//		at com.sun.jmx.interceptor.DefaultMBeanServerInterceptor.registerMBean(DefaultMBeanServerInterceptor.java:324)
//		at com.sun.jmx.mbeanserver.JmxMBeanServer.registerMBean(JmxMBeanServer.java:522)
//		at org.apache.kafka.common.utils.AppInfoParser.registerAppInfo(AppInfoParser.java:58)
//		at org.apache.kafka.clients.producer.KafkaProducer.<init>(KafkaProducer.java:331)
//		at org.apache.kafka.clients.producer.KafkaProducer.<init>(KafkaProducer.java:188)
//		at com.us.demo.kafka.orginal.Producer.<init>(Producer.java:23)
//		at com.us.demo.kafka.orginal.SimpleConsumerDemo.generateDate(SimpleConsumerDemo.java:32)
//		at com.us.demo.kafka.orginal.SimpleConsumerDemo.main(SimpleConsumerDemo.java:42)

//http://stackoverflow.com/questions/8919793/javax-management-instancealreadyexistsexception-when-using-hadoop-minidfscluster
// http://stackoverflow.com/questions/38108963/apache-spark-getting-a-instancealreadyexistsexception-when-running-the-kafka-pr