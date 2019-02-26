package com.mykafka.examples;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

// import com.lamfire.logger.Logger;

/**
 * kafka-clients=0.8.2.0
 *
 * @author zxc Mar 30, 2017 2:44:05 PM
 */
public class ProducerTest {

    // protected static Logger     log          = Logger.getLogger(ProducerTest.class);

    protected static Properties props        = new Properties();
    protected static String     kafka_server = "192.168.180.154:9092,192.168.180.155:9092";
    protected static String     topic        = "zxc_test";

    static {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_server);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);
    }

    public static void main(String[] args) throws Exception {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i)), //
                    new org.apache.kafka.clients.producer.Callback() {

                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e == null) {
                                System.out.println("offset=" + recordMetadata.offset() + ",partition=" + recordMetadata.partition());
                            } else {
                                System.out.println("send error!"+e.toString());
                            }
                        }
                    });
        }
        TimeUnit.SECONDS.sleep(3);
        producer.close();
    }
}