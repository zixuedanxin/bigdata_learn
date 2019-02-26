package com.yanxml.kafka.stresstest.config;

public class StressEnvConfig {
	
	public static final String jdbc_driverClassName="com.mysql.jdbc.Driver";
	public static final String jdbc_url="jdbc:mysql://localhost:3306/flume_test?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true";
	public static final String jdbc_username="root";
	public static final String jdbc_password="admin";
	public static final String jdbc_connection_maxPoolSize="20";
	public static final String jdbc_connection_minPoolSize="20";
	public static final String jdbc_connection_initialPoolSize="20";
	
	public static final String kafka_url="192.168.100.87:9093,192.168.100.88:9093,192.168.100.89:9093";
//	public static final String kafka_topics="bmc-events-topic-dev";
	public static final String kafka_topics="hello-sean";
	public static final String consumer_groups="20";



}
