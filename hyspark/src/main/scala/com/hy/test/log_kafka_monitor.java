package com.hy.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class log_kafka_monitor {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        //kafka服务器地址
        props.put("bootstrap.servers",  "broker1:9092,broker2:9092");
        //ack是判断请求是否为完整的条件（即判断是否成功发送）。all将会阻塞消息，这种设置性能最低，但是最可靠。
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        props.put("acks", "1");
        //retries,如果请求失败，生产者会自动重试，我们指定是0次，如果启用重试，则会有重复消息的可能性。
        props.put("retries", 2);
        //producer缓存每个分区未发送消息，缓存的大小是通过batch.size()配置设定的。值较大的话将会产生更大的批。并需要更多的内存(因为每个“活跃”的分区都有一个缓冲区)
        props.put("batch.size", 16384);
        //默认缓冲区可立即发送，即便缓冲区空间没有满；但是，如果你想减少请求的数量，可以设置linger.ms大于0.这将指示生产者发送请求之前等待一段时间
        //希望更多的消息补填到未满的批中。这类似于tcp的算法，例如上面的代码段，可能100条消息在一个请求发送，因为我们设置了linger时间为1ms，然后，如果我们
        //没有填满缓冲区，这个设置将增加1ms的延迟请求以等待更多的消息。需要注意的是，在高负载下，相近的时间一般也会组成批，即使是linger.ms=0。
        //不处于高负载的情况下，如果设置比0大，以少量的延迟代价换取更少的，更有效的请求。
        props.put("linger.ms", 1);
        //buffer.memory控制生产者可用的缓存总量，如果消息发送速度比其传输到服务器的快，将会耗尽这个缓存空间。当缓存空间耗尽，其他发送调用将被阻塞，阻塞时间的阈值
        //通过max.block.ms设定，之后他将抛出一个TimeoutExecption。
        props.put("buffer.memory", 33554432);
        //key.serializer和value.serializer示例：将用户提供的key和value对象ProducerRecord转换成字节，你可以使用附带的ByteArraySerizlizaer或StringSerializer处理简单的byte和String类型.
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //设置kafka的分区数量
        props.put("kafka.partitions", 2);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"mooc\" password=\"moocpswd\";");
        props.put("sasl.kerberos.service.name", "kafka");
        Producer<String, String> producer = new KafkaProducer<>(props);
        HashMap<String, String> rows=new HashMap<String, String>();
        String topic="app_log";
        for (int i = 0; i < 500000; i++){
        // while (true){
            List<HashMap<String, String>>  listMaps = new ArrayList<HashMap<String, String>> ();
            Long nows=System.currentTimeMillis();
            rows.put("pfrm_nm","andriod");
            rows.put("app_nm","掌上海银");
            rows.put("uuid","uuid"+String.valueOf((nows/100000)));
            rows.put("user_id","user"+String.valueOf(nows%100000));
            rows.put("curr_page_id","pageid"+String.valueOf(nows%100));
            rows.put("cat_id","cat"+String.valueOf(nows%10));
            rows.put("curr_page_tag",String.valueOf(nows%100));
            rows.put("to_url","http:"+String.valueOf(nows%1000000));
            rows.put("dmn_nm","web.test");
            rows.put("act_cmnt","test"+String.valueOf(nows%1000));
            rows.put("act_type","a");
            rows.put("crt_dtm",String.valueOf(nows));
            listMaps.add(rows);
            HashMap<String, String> rows2=(HashMap<String, String> )rows.clone();
            rows2.put("act_type","b");
            listMaps.add(rows2);
            HashMap<String, List> rs=new HashMap<String, List>();
            rs.put("data",listMaps);
            System.out.println(new JSONObject(rs).toString());
            producer.send(new ProducerRecord<String, String>(topic, String.valueOf(nows), new JSONObject(rs).toString()));
            rows.clear();
            Thread.sleep(600000);
        }

        producer.close();
    }
}
