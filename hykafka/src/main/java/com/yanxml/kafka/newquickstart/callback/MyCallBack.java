package com.yanxml.kafka.newquickstart.callback;

import java.util.Date;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 用于获取从kafka发送消息的返回值。
 * 
 * */
public class MyCallBack implements Callback{
	private Date time;
	private String topicName;
	private String data;
	private String clusterInfo;
//	Logger

	public MyCallBack(String topicName,String data,String clusterInfo){
		this.time = new Date();
		this.topicName = topicName;
		this.data = data;
		this.clusterInfo = clusterInfo;
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if(null != metadata){
			System.out.println(" ClusterInfo: "+ clusterInfo + " Topic: "+ metadata.topic() + "Time:"+time+" Data: "+data);
		}else{
//			System.out.println("");
			//exception.printStackTrace();
		}
	}

}
