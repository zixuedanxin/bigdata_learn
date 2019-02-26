package com.zxm.st;

import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerThread implements Runnable {
	private QuotationProducer producer;
	private ProducerRecord<String,String> record;
	public KafkaProducerThread() {

	}

	public KafkaProducerThread(QuotationProducer p,ProducerRecord<String,String> r) {
		this.producer = p;
		record = r;
	}

	@Override
	public void run() {

		producer.sendAsync(record);

	}

}
