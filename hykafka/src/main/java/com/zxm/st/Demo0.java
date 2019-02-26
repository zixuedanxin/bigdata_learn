package com.zxm.st;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

public class Demo0 {
	private static final int MSG_SIZE = 10;
	private static final String TOPIC = "stock-quotation";

	public static void main(String[] args) {
		ProducerRecord<String, String> record = null;
		StockQuotationInfo info = null;
		QuotationProducer producer = new QuotationProducer();
		try {
			int num = 0;
			for (int i = 0; i < MSG_SIZE; i++) {
				info = producer.createQuotationInfo();
				record = new ProducerRecord(TOPIC, null, info.getTradeTime(), info.getStockCode(), info.toString());
				producer.sendAsync(record);
				/*if (num++ % 10 == 0) {
					Thread.sleep(2000);
				}*/
			}
		} catch (/*Interrupted*/Exception e) {
			e.printStackTrace();
		}finally {
			producer.close();
		}
	}

}
