package com.yanxml.kafka.simple;

public class KafkaConfig {
	// zookeeper
	// public static final String zookeeper_list_dev="";
	public static final String zookeeper_list_qa="192.168.100.77:2181,192.168.100.78:2181,192.168.100.79:2181";
	
	// broker
	// public static final String broker_list_dev="";
	public static final String broker_list_qa="192.168.100.71:9092,192.168.100.72:9092,192.168.100.73:9092,192.168.100.74:9092,192.168.100.75:9092,192.168.100.76:9092";
	
	public static String zookeeper_list=zookeeper_list_qa;
	public static String broker_list=broker_list_qa;
	
	//public static final String producer_topic_dev="";
	public static final String producer_topic_qa="sean";
	
	//public static final String consumer_topic_dev="";
	public static final String consumer_topic_qa="sean";
	
	public static String producer_topic=producer_topic_qa;
	public static String consumer_topic=consumer_topic_qa;
	
	public static final String Consumer_groupId = "kafka-streaming-test";

	public static final String Consumer_zookeeper_session_timeout_ms = "40000";
	public static final String Consumer_zookeeper_sync_time_ms = "200";
	public static final String Consumer_auto_commit_interval_ms = "1000";	

}
