package com.zxm.st;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.*;

public class QuotationProducer {

	private static final Logger LOGGER = Logger.getLogger(QuotationProducer.class);
	private static final int MSG_SIZE = 100;
	private static final String TOPIC = "stock-quotation";
	private static final String BROKER_LIST = "hadoop11:9092,hadoop12:9092,hadoop13:9092";

	private static KafkaProducer<String, String> producer = null;

	static {
		Properties configs = initConfig();
		producer = new KafkaProducer<String, String>(configs);
	}

	private static Properties initConfig() {
		Properties properties = new Properties();
		properties.put(BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
		properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}

	public StockQuotationInfo createQuotationInfo() {
		StockQuotationInfo info = new StockQuotationInfo();
		Random r = new Random();
		Integer stockCode = 600100 + r.nextInt(10);
		float random = (float) Math.random();
		if (random / 2 < 0.5) {
			random = -random;
		}

		DecimalFormat decimalFormat = new DecimalFormat(".00");
		info.setCurrentPrice(Float.valueOf(decimalFormat.format(11 + random)));
		info.setPreClosePrice(11.80f);
		info.setOpenPrice(11.5f);
		info.setLowPrice(10.5f);
		info.setHighPrice(12.5f);
		info.setStockCode(stockCode.toString());
		info.setTradeTime(System.currentTimeMillis());
		info.setStockName("Stock-" + stockCode);

		return info;
	}

	public void sendAsync(ProducerRecord<String, String> r) {
		LOGGER.info("Begin sending ... ");
		producer.send(r, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception != null) {
					LOGGER.error("Exception in sendAsync:" + exception.getMessage(), exception);
				}
				
				if(metadata!=null) {
					LOGGER.info(String.format("offset:%s,partition:%s",metadata.offset(),metadata.partition()));
				}
				
				
			}
		});
	}

	public void close() {
		this.producer.close();
	}

}
