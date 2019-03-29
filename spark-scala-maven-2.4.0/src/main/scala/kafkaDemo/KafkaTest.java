package kafkaDemo;

/**
 * Created by dell on 2019/2/7.
 */
public class KafkaTest {

    public static void main(String[] args){
        new KafkaProducer(KafkaProperties.TOPIC).start();
    }

}
