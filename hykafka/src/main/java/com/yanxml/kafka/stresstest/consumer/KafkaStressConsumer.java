package com.yanxml.kafka.stresstest.consumer;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.yanxml.kafka.stresstest.config.StressEnvConfig;
import com.yanxml.kafka.stresstest.dao.DataHandleDao;
import com.yanxml.kafka.stresstest.util.RegexUtil;

public class KafkaStressConsumer {
	
	public KafkaConsumer<String, String> consumer;
	public DataHandleDao dataHandleDao;


	// 构造函数
	public KafkaStressConsumer() {
		// Handle Dao
		dataHandleDao = new DataHandleDao();
		
		Properties props = new Properties();
		System.setProperty("java.security.auth.login.config",
				"/Users/Sean/Documents/Gitrep/bigdata/kafka/src/main/resources/kafka_client_jaas.conf"); // 环境变量添加，需要输入配置文件的路径
		props.put("security.protocol", "SASL_PLAINTEXT");
		props.put("sasl.mechanism", "PLAIN");
		props.put("bootstrap.servers", StressEnvConfig.kafka_url);
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("group.id", "hello-sean-group");
		props.put("auto.offset.reset", "latest");
		props.put("enable.auto.commit", true);
		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(StressEnvConfig.kafka_topics));// 订阅的Topic 可以写多个
																// "TopicA","TopicB","TopicC"
	}

	// 消费数据
	public void consume() {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records){
				System.out.printf("offset = %d, key = %s, value = %s%n",
						record.offset(), record.key(), record.value());
				String content = record.value();
				String out_id_str = (RegexUtil.regex("\\[(.*?)\\{", (content), 1));
//[12{"jsonString":"{\"event_handle\": \"xxxxxxxxxx\",\"SOURCE_IP\": \"xx.xxx.xx.x:xxxx\",\"AREA\": \"xx中心\",\"date\": \"2018-06-25 15:49:42\",\"status\": \"OPEN\",\"mc_object_class\": \"mc_object_class\",\"mc_parameter\": \"ConnectionStatus\",\"mc_object\": \"xx.xxx.xx.xxx\",\"mc_host_address\": \"xxx.xxx.xxx.x\",\"mc_parameter_value\": \"85.06\",\"severity\": \"MAJOR\",\"mc_tool_class\": \"xxxx_xxxx\",\"mc_appname\": \"mc_appname\",\"updatetime\": \"2018-06-25 15:49:42\",\"msg\": \"xx使用率为71%\",\"xx\": \"xxx_1xx\"}"}]
//				12{"jsonString":"
//				Integer out_id = Integer.parseInt(out_id_str);
				Integer out_id = 0;
				dataHandleDao.insertAlert(out_id,content);
			}
				
		}
	}

}
