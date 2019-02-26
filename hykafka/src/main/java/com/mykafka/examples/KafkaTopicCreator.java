package com.mykafka.examples;

import java.util.Properties;
import org.apache.kafka.common.security.JaasUtils;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zxc Mar 15, 2017 5:23:36 PM
 */
public class KafkaTopicCreator {
    public static void createKafaTopic() {

        ZkUtils zkUtils = ZkUtils.apply(
                "172.30.251.331:2181,172.30.251.341:2181", 30000, 30000,
                JaasUtils.isZkSecurityEnabled());
        Properties conf = new Properties();
        // AdminUtils.createTopic(zkUtils, bean.getTopic(), bean.getPartition(), bean.getReplication(), new Properties(), new RackAwareMode.Enforced$());
    }


    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicCreator.class);

    public static void main(String[] args) {
        String topicName = "zxc_test";
        String zookeeperHosts = "broker1:2181,broker2:2181";
        String kafkaBrokerHosts = "broker1:9092,broker2:9092";
        int sessionTimeOut = 10000;
        int connectionTimeOut = 10000;
        LOGGER.info("zookeeperHosts:{}", zookeeperHosts);
        ZkClient zkClient = new ZkClient(zookeeperHosts, sessionTimeOut, connectionTimeOut, ZKStringSerializer$.MODULE$);
//        if (!AdminUtils.topicExists(zkClient, topicName)) {
//            int replicationFactor = kafkaBrokerHosts.split(",").length;
//            AdminUtils.createTopic(zkClient, topicName, 1, replicationFactor, new Properties());
//        } else {
//            LOGGER.info("{} is available hence no changes are done");
//        }
//        LOGGER.info("Topic Details:{}", AdminUtils.fetchTopicMetadataFromZk(topicName, zkClient));
    }
}