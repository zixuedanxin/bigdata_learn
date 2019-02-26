package com.yanxml.kafka.simple;

/**
 * 高层流DSL
 * Kafka Stream提供两种开发流处理拓扑（stream processing topology）的API

    1、high-level Stream DSL：提供通用的数据操作，如map和filter
    2、lower-level Processor API：提供定义和连接自定义processor，同时跟state store（下文会介绍）交互

 * */
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;

public class WordCountDemo {
	public static void main(String []args) throws Exception{
		Properties props=new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
		//String zookeep_qa="192.168.100.77:2181,192.168.100.78:2181,192.168.100.79:2181";
		//String broker_lists_qa="192.168.100.71:9092,192.168.100.72:9092,192.168.100.73:9092,192.168.100.74:9092,192.168.100.75:9092,192.168.100.76:9092";
		// props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaConfig.broker_list);
		// props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, KafkaConfig.zookeeper_list);
		// props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
		// Serdes.String().getClass().getName() 
		// org.apache.kafka.common.serialization.Serdes$StringSerde@5451c3a8
		// class org.apache.kafka.common.serialization.Serdes$StringSerde
		// org.apache.kafka.common.serialization.Serdes$StringSerde
		// System.out.println(Serdes.String());
		// System.out.println(Serdes.String().getClass());
		// System.out.println(Serdes.String().getClass().getName());
        props.put("bootstrap.servers", "broker1:9092,broker2:9092");
        //消费者群组ID，发布-订阅模式，即如果一个生产者，多个消费者都要消费，那么需要定义自己的群组，同一个群组内的消费者只有一个能消费到消息
        props.put("group.id", "test2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //true，消费者的偏移量将在后台定期提交；false关闭自动提交位移，在消息被完整处理之后再手动提交位移
        props.put("enable.auto.commit", "true");
        //如何设置为自动提交（enable.auto.commit=true）,这里设置自动提交周期
        props.put("auto.commit.interval.ms", "1000");
        //session.timeout.ms:在使用kafka的组管理时，用于检测消费者故障的超时
        props.put("session.timeout.ms", "30000");
        //key.serializer和value.serializer示例：将用户提供的key和value对象ProducerRecord转换成字节，你可以使用附带的ByteArraySerizlizaer或StringSerializer处理简单的byte和String类型.
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"mooc\" password=\"moocpswd\";");
        props.put("sasl.kerberos.service.name", "kafka");
//		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		
		KStreamBuilder builder=new KStreamBuilder();
		// 从abel节点 获取数据
		KStream<String,String> source = builder.stream("app_log");

		// KStream<String,String> source = builder.stream("streams-file-input");
		//Serdes 串行解串器
		KTable<String, Long> counts=source
				.flatMapValues(new ValueMapper<String, Iterable<String>>() {
					// Iterable 可迭代的
					public Iterable<String> apply(String value){
						return Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" "));
					}
				}).map(new KeyValueMapper<String, String, KeyValue<String,String>>() {
					public KeyValue<String,String> apply(String key,String value){
						return new KeyValue<>(value,value);
					}
				})
				//.countByKey("Counts");
         .groupByKey()
         .count("Counts");
		
        // need to override value serde to Long type
		// to() 方法就是将Topic 又发回容器 也就是topic 这个topic的名字叫做 streams-wordcount-output
		counts.print();
		//counts.to(Serdes.String(),Serdes.Long(),"streams-wordcount-output");
		
		KafkaStreams streams=new KafkaStreams(builder, props);
		streams.start();
		
		Thread.sleep(5000);
		
		//streams.close();
		
	}

}
