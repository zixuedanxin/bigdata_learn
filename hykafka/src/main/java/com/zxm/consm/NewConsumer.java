package com.zxm.consm;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import kafka.common.OffsetAndMetadata;

public class NewConsumer {
	
	private static final Logger LOGGER = Logger.getLogger(NewConsumer.class);
	private static final Properties PROPERTIES = new Properties();
	private static final KafkaConsumer<String, String> consumer;

	static {
		PROPERTIES.put("bootstrap.servers", "192.168.200.11:9092");
		PROPERTIES.put("group.id", "mytest");
		PROPERTIES.put("client.id", "mytest");
		PROPERTIES.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		PROPERTIES.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		consumer = new KafkaConsumer<>(PROPERTIES);
	}

	public void subscribe(String topic) {
		consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {				
				LOGGER.info("rebalance begins ... ");
				consumer.commitAsync();
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				LOGGER.info("rebalance ends ... ");
				long commitedOffset = -1;
				for (TopicPartition p : partitions) {
					org.apache.kafka.clients.consumer.OffsetAndMetadata offsetAndMetadata = consumer.committed(p);
					commitedOffset = offsetAndMetadata.offset();
					consumer.seek(p, commitedOffset + 1);
				}
			}
		});
	}

	public ConsumerRecords<String, String> poll(long timeout) {
		ConsumerRecords<String, String> res = consumer.poll(timeout);
		return res;
	}
	
	public static void main(String[] args) {
		NewConsumer consumer = new NewConsumer();
		consumer.subscribe("stock-quotation");
	/*	ConsumerRecords<String, String> msgs= consumer.poll(10*1000);
		System.out.println(msgs.count());
		Iterator<ConsumerRecord<String,String>>it= msgs.iterator();
		System.out.println(it.next().key());*/
		while(true) {
			ConsumerRecords<String, String> msgs= consumer.poll(10*1000);
			Iterator<ConsumerRecord<String,String>> it= msgs.iterator();
			while(it.hasNext()) {
				ConsumerRecord<String,String> r = it.next();
				System.out.println(r.key()+":"+r.value());
			}
		}
	}

}
