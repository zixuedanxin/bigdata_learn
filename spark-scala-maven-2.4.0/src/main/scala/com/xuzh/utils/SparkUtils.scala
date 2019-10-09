package com.xuzh.utils

import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils



class SparkUtils(ssc: StreamingContext) {

  def getDirectStream(topics: Set[String]): InputDStream[(String, String)] = {

    def readFromOffsets(list: List[(String, Int, Int)]): Map[TopicAndPartition, Long] = {
      var fromOffsets: Map[TopicAndPartition, Long] = Map()
      for (offset <- list) {
        val tp = TopicAndPartition(offset._1, offset._2)
        fromOffsets += (tp -> offset._3)
      }
      fromOffsets
    }

    val dauTopic = topics.head
    val dauOffsetList = List(
      (dauTopic, 1, 1651983),
      (dauTopic, 2, 1625775),
      (dauTopic, 3, 1625780),
      (dauTopic, 4, 1625761),
      (dauTopic, 5, 1651990),
      (dauTopic, 6, 1625791),
      (dauTopic, 7, 1625776),
      (dauTopic, 8, 1625766)
    )

    val dauFromOffsets = readFromOffsets(dauOffsetList)
    val dauMessageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)

    val size = dauOffsetList.size
    val inputDS = null
//    val inputDS: InputDStream[(String, String)] = if (size < 0) {
//      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
//        ssc, ParamsUtils.kafka.KAFKA_PARAMS, dauFromOffsets, dauMessageHandler)
//    } else {
//      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//        ssc, ParamsUtils.kafka.KAFKA_PARAMS, ParamsUtils.kafka.KAFKA_TOPIC)
//    }

     inputDS

  }

}
