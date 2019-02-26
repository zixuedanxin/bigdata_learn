package com.zxm.st;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.*;

public class Demo1 {

	private static final int MSG_SIZE = 100;
	private static final String TOPIC = "stock-quotationB";

	public static void main(String[] args) {

		ExecutorService es = Executors.newFixedThreadPool(5);

		ProducerRecord<String, String> record = null;
		StockQuotationInfo info = null;
		QuotationProducer producer = new QuotationProducer();

		try {
			int num = 0;
			List<Future> rl = new ArrayList<Future>();
			for (int i = 0; i < 100; i++) {
				info = producer.createQuotationInfo();
				record = new ProducerRecord(TOPIC, null, info.getTradeTime(), info.getStockCode(),
						i + "-" + info.toString());
				Future r = es.submit(new KafkaProducerThread(producer, record));
				rl.add(r);
				// System.err.println("i:"+i);
				/*
				 * producer.sendAsync(record); if (num++ % 10 == 0) { Thread.sleep(2000); }
				 */
			}
			for(int i=0;i<rl.size();i++) {
				System.out.println(rl.get(i).get());
			}
			System.err.println("END");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
			es.shutdown();
		}
	}
}
