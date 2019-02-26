package com.multi.store.manage.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @date 2018/9/18 9:52.
 */
@RestController
@RequestMapping(value = "kafka")
public class KafkaController {
    @Autowired
    private KafkaTemplate kafkaTemplate;


    @RequestMapping(value = "sendMessage")
    public String sendMessage(@RequestBody String msgJson){
        kafkaTemplate.send("ota-topics",msgJson);
        return "成功";
    }
}
