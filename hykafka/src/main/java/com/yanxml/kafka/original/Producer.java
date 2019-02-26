package com.yanxml.kafka.original;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class Producer extends Thread{
	private final KafkaProducer<Integer, String> producer;
	private final String topic;
	//private final String clientId;
	private final Boolean isAsync;//是否异步
	
	// kafka对象的初始化
		public Producer(String topic,Boolean isAsync){
			Properties props=new Properties();
//			props.put("bootstrap.servers", "127.0.0.1:9092");
			props.put("bootstrap.servers", "112.64.171.218:9092");
			//props.put("bootstrap.servers", "139.224.133.152:9092");
			//props.put("bootstrap.servers", KafkaConfig.broker_list);
			props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			producer=new KafkaProducer<>(props);
			this.topic=topic;
			this.isAsync=isAsync;
		}
		
	// kafka对象的初始化
	public Producer(String topic,Boolean isAsync,String clientId){
		Properties props=new Properties();
		props.put("bootstrap.servers", "139.224.133.152:9092");

		//props.put("bootstrap.servers", "139.224.133.152:9092");
		//props.put("bootstrap.servers", KafkaConfig.broker_list);
		props.put("client.id", clientId);//?? client.id是什么意思 用来唯一标识 producer对象的id
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer=new KafkaProducer<>(props);
		this.topic=topic;
		this.isAsync=isAsync;
	}
	
	public void run(){
		int messageNo=1;
		while(true){
			String messageStr="Message_"+messageNo;
			long startTime=System.currentTimeMillis();// Get the now time
			if(isAsync){
				//异步发送数据
				producer.send(new ProducerRecord<>(topic,messageNo, messageStr), 
						new DemoCallBack(startTime, messageNo, messageStr));
			}else{
				//同步发送数据
				try{
					producer.send(new ProducerRecord<>(topic,messageNo,messageStr)).get();
					System.out.println(" Send Message:("+messageNo+","+messageStr+")");
				}catch(InterruptedException|ExecutionException e){
					e.printStackTrace();
				}
			}
			messageNo++;
		}
	}
	class DemoCallBack implements Callback{
		
		private final long startTime;
		private final int key;
		private final String message;
		
		public DemoCallBack(long startTime,int key,String message){
			this.startTime=startTime;
			this.key=key;
			this.message=message;
		}

		@Override
	    /**
	     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
	     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
	     * non-null.
	     *
	     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
	     *                  occurred.
	     * @param exception The exception thrown during processing of this record. Null if no error occurred.
	     */
		public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
			long elapsedTime=System.currentTimeMillis()-startTime;//elapsedTime 逝去的时间
			if(recordMetadata!=null){
				System.out.println(
						"message:{ "+key+","+message+"} send to partition ("+ recordMetadata.partition()+")"
						+" offset ("+recordMetadata.offset()+")"+" elapsedTime ("+elapsedTime+" ms )"
				);
			}else{
				exception.printStackTrace();
			}
		}
	}
	
	public static void main(String []args){
		// 需要1 开启本地kafka 2.在本地kafka上创建一个名称为sean的topic
		 //Producer producerThread = new Producer("sean", true,"DemoProducer");
		Producer producerThread = new Producer("test", true);

	     producerThread.start();
	}
	

}
