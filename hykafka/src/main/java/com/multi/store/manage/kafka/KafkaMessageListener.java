package com.multi.store.manage.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * kafka监听
 * @date 2018/8/20 18:31.
 */
public class KafkaMessageListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageListener.class);
    /**
     *
     * @createDate:2018/8/20 18:40
     * @return
     */
    @KafkaListener(topics = {"test-topics"})
    public void linstenOtaStatus(ConsumerRecord<?, ?> record){
        LOGGER.info("test-topics -->kafka监听到的值为：{}",record.value().toString());
    }
}
