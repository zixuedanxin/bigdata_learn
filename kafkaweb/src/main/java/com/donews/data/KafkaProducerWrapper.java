package com.donews.data;

import com.typesafe.config.Config;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.Properties;

/**
 * Created by reynold on 16-6-22.
 *
 */
public class KafkaProducerWrapper {
    private Logger LOG = LoggerFactory.getLogger(KafkaProducerWrapper.class);
    private KafkaProducer<String, String> producer = init();

    private KafkaProducer<String, String> init() {
        Config conf = Configuration.conf.getConfig("kafka");
        Properties props = new Properties();
        props.put("bootstrap.servers", conf.getString("bootstrap.servers"));
        props.put("acks", conf.getString("acks"));
        props.put("retries", conf.getInt("retries"));
        props.put("batch.size", conf.getInt("batch.size"));
        props.put("linger.ms", conf.getInt("linger.ms"));
        props.put("buffer.memory", conf.getLong("buffer.memory"));
        props.put("key.serializer", conf.getString("key.serializer"));
        props.put("value.serializer", conf.getString("value.serializer"));
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",conf.getString("sasl.jaas.config"));
               // "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"mooc\" password=\"moocpswd\";");
        props.put("sasl.kerberos.service.name", "kafka");
        //LOG.info("KafkaProducer Properties: {0}", props.toString());
        return new KafkaProducer<>(props);
    }

    public void send(String topic, String message, Callback callback) {
        producer.send(new ProducerRecord<>(topic, message), callback);
    }

    public void close() {
        producer.close();
        LOG.info("Kafka Producer Closed");
    }

    public static void main(String[] args) {
        //KafkaProducerWrapper sender=new KafkaProducerWrapper();
        //sender.producer.partitionsFor("xxxxx").forEach(System.out::println);
    }
}