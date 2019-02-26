package com.yanxml.kafka.stresstest;

import com.yanxml.kafka.stresstest.consumer.KafkaStressConsumer;

/**
 * 压力测试入口。用于将Kafka内的数据读取出来，进行比对。
 * (1. 查看Kafka内是否多出数据。2. 查看Kafka内是否少了数据。 3. 查看写入Kafka内数据的速率。)
 * */
public class Main {
	public static void main(String[] args) {
		new KafkaStressConsumer().consume();
	}

}
