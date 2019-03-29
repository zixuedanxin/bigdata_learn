import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


object Kafka_Word_Count {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val ssc = new StreamingContext("local[*]", " kafka_word_count", Seconds(1) )

    val kafkaParams = Map("bootstrap.servers" -> "localhost:9092 , localhost:9093" , "group.id" -> "org.practise.kafka_word_count" ,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

     val topics = List("word_count").toSet

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val lines = stream.map(record => (record.value))

    val count = lines.flatMap(data => (data.split(" "))).map(data => (data,1)).reduceByKeyAndWindow( _ + _, _ - _, Seconds(300), Seconds(1))
    count.transform(rdd => rdd.sortBy(data => data._2,false)).print()

    // Kick it off
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()


  }

}
