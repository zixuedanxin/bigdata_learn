package com.yanxml.kafka.quickstart;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;  
import kafka.serializer.StringEncoder;  
 
/**
 * 
 * @author yangyibo
 *
 */
public class KafkaOldProducer extends Thread{
	 private String topic;  
     
	    public KafkaOldProducer(String topic){
	        super();  
	        this.topic = topic;  
	    }  
	      
	      
	    @Override  
	    public void run() {


	        kafka.javaapi.producer.Producer producer = createProducer();
	        int i=0;  
	        while(i <= i+10000){

	              producer.send(
						  new KeyedMessage<Integer, String>(topic, "message: 我是第"+ i+"条信息"+" abel"));
				i++;
				try {
	                TimeUnit.SECONDS.sleep(3);  
	            } catch (InterruptedException e) {  
	                e.printStackTrace();  
	            }  
	        }  
	    }  
	  
		private kafka.javaapi.producer.Producer createProducer() {
	        Properties properties = new Properties(); 
//		    properties.put("zookeeper.connect", "127.0.0.1:2181");//声明zk  
	        
		    properties.put("zookeeper.connect", "192.168.1.16:2181");//声明zk  

//		    properties.put("zookeeper.connect", "127.0.0.1:2181，127.0.0.1:2182，127.0.0.1:2183");//声明zk  


//		    properties.put("zookeeper.connect", "139.224.133.152:2181");//声明zk  

	       // properties.put("zookeeper.connect", KafkaConfig.zookeeper);//声明zk  
	        properties.put("serializer.class", StringEncoder.class.getName());  
	       // properties.put("metadata.broker.list", KafkaConfig.metadata_broker_list);// 声明kafka broker
//	        properties.put("metadata.broker.list", "hello:9092,hello:9093,hello:9094");// 声明kafka broker
//	        properties.put("metadata.broker.list", "192.168.1.16:9092,192.168.1.16:9093,192.168.1.16:9094");// 声明kafka broker
//	        properties.put("metadata.broker.list", "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");// 声明kafka broker

	        properties.put("metadata.broker.list", "192.168.1.16:9094");// 声明kafka broker

//	        properties.put("metadata.broker.list", "kafka2:9092");// 声明kafka broker

//	        properties.put("metadata.broker.list", "192.168.1.16:9092");// 声明kafka broker

	        //	        properties.put("metadata.broker.list", "192.168.100.52:9092");// 声明kafka broker
//	        properties.put("metadata.broker.list", "139.224.133.152:9092");// 声明kafka broker

	        return new kafka.javaapi.producer.Producer(new ProducerConfig(properties));
	        
	     }  
	      
	      
		/**
		 * 
		 * @param topicName
		 */
	    public static void creatProducer(String topicName) {  
	        new KafkaOldProducer(topicName).start();// 使用kafka集群中创建好的主题 test
	    }  
	      
	    public static void main(String[] args) {
	    	creatProducer("test5");
		}
	} 