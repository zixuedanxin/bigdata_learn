/*
# Program      : KafkaConfigUtils.scala
# Date Created : 23/10/2018
# Description  : This is the used get the kafka producer/producer config
# Parameters   :
#
# Modification history:
#
# Date         Author               Description
# ===========  ===================  ============================================
# 23/10/2018   Anand Ayyasamy        Creation
# ===========  ===================  =============================================
*/
package com.struct.stream.utils

import java.util.Properties

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.mutable

object KafkaConfigUtils {

  /*Kafka prefix value for each property*/
  private val KAFKA_PREFIX="kafka."
 /* truststore location*/
  private val TRUSTSTORE_LOC="ssl.truststore.location"
 /* truststore password*/
  private val TRUSTSTORE_PWD="ssl.truststore.password"
  /*kerberos service name*/
  private val KAFKA_SERVICE_NAME="sasl.kerberos.service.name"
  /*starting offset*/
  private val STARTING_OFFSET="kafka.startingOffsets"
  /*consumer topics*/
  final val CONSUMER_TOPICS="kafka.consumer.topics"
  /*producer topics*/
  final val PRODUCER_TOPICS="kafka.producer.topics"

  /**
    * Consumer Cofig
    * @param props
    * @return
    */
  def getConsumerKeyObj(props: mutable.Map[String, String]):Map[String, Object] = {
    val params = Map[String, Object](
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> props(KAFKA_PREFIX+CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> props(KAFKA_PREFIX+ConsumerConfig.GROUP_ID_CONFIG),
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> props(KAFKA_PREFIX+ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (props(KAFKA_PREFIX+ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).toString.toBoolean: java.lang.Boolean),
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> props(KAFKA_PREFIX+CommonClientConfigs.SECURITY_PROTOCOL_CONFIG),
      TRUSTSTORE_LOC -> props(KAFKA_PREFIX+TRUSTSTORE_LOC),
      TRUSTSTORE_PWD -> props(KAFKA_PREFIX+TRUSTSTORE_PWD),
      KAFKA_SERVICE_NAME -> props(KAFKA_PREFIX+KAFKA_SERVICE_NAME))
    params
  }

  /**
    * Producer Config
    * @param props
    * @return
    */
  def getProducerProps(props: mutable.Map[String, String]):Properties = {
    val prop = new Properties()
    prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,props(KAFKA_PREFIX+CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(KAFKA_SERVICE_NAME,props(KAFKA_PREFIX+KAFKA_SERVICE_NAME))
    prop.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG , props(KAFKA_PREFIX+CommonClientConfigs.SECURITY_PROTOCOL_CONFIG))
    prop.put(TRUSTSTORE_LOC,props(KAFKA_PREFIX+TRUSTSTORE_LOC))
    prop.put(TRUSTSTORE_PWD ,props(KAFKA_PREFIX+TRUSTSTORE_PWD))
    prop
  }



  /**
    * Consumer Config used for Structured streaming
    * @param props
    * @return
    */
  def getConsumerMap(props: mutable.Map[String, String]):Map[String, String] = {
    val params = Map[String, String](
     KAFKA_PREFIX+CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> props(KAFKA_PREFIX+CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
    "subscribe" -> props(CONSUMER_TOPICS),
    STARTING_OFFSET ->  props(STARTING_OFFSET),
     "failOnDataLoss" ->"false",
      "maxOffsetsPerTrigger" -> "1",
    KAFKA_PREFIX+CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> props(KAFKA_PREFIX+CommonClientConfigs.SECURITY_PROTOCOL_CONFIG),
    KAFKA_PREFIX+KAFKA_SERVICE_NAME ->props(KAFKA_PREFIX+KAFKA_SERVICE_NAME),
    KAFKA_PREFIX+TRUSTSTORE_LOC -> props(KAFKA_PREFIX+TRUSTSTORE_LOC),
    KAFKA_PREFIX+TRUSTSTORE_PWD ->props(KAFKA_PREFIX+TRUSTSTORE_PWD))
    params
  }

  /**
    * Producer Config used for Structured streaming
    * @param props
    * @return
    */
  def getProducerMap(props: mutable.Map[String, String]): Map[String, String] = {
    val params = Map[String, String](
      KAFKA_PREFIX+CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> props(KAFKA_PREFIX+CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
      KAFKA_PREFIX+KAFKA_SERVICE_NAME ->props(KAFKA_PREFIX+KAFKA_SERVICE_NAME),
      KAFKA_PREFIX+CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> props(KAFKA_PREFIX+CommonClientConfigs.SECURITY_PROTOCOL_CONFIG),
      KAFKA_PREFIX+TRUSTSTORE_LOC -> props(KAFKA_PREFIX+TRUSTSTORE_LOC),
      KAFKA_PREFIX+TRUSTSTORE_PWD ->props(KAFKA_PREFIX+TRUSTSTORE_PWD),
      "topic" -> props(PRODUCER_TOPICS)
    )
    params
  }




}