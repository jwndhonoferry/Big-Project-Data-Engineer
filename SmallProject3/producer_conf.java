package consumer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public class producer_conf {

    public static final String SERVERS = "localhost:9092";
    public static final String TOPIC = "tweet_spotify";

}
