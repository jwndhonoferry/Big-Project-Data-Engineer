package Properties;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class all_props {
    // Consumer Configuration Settings
    public static Consumer<Long,String>createConsumer(String topic) throws IOException
    {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, conf.GROUP_ID_C);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.parseInt(conf.MAX_POLL));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        Consumer<Long,String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    // Kafka Parameters for Spark Streaming Configuration Settings
    public static HashMap<String, Object> kafkaParams() throws IOException{
        HashMap<String,Object> params = new HashMap<>();
        params.put("bootstrap.servers", conf.BOOTSTRAP_SERVERS);
        params.put("key.deserializer", StringDeserializer.class.getName());
        params.put("value.deserializer", StringDeserializer.class.getName());
        params.put("group.id", conf.GROUP_ID_SS);
        params.put("auto.offset.reset", conf.AUTO_OFSET);
        params.put("enable.auto.commit",conf.AUTO_COMIT);
        return params;
    }

    // Producers Configuration Settings
    public static Producer<Long, String> createProducer() throws IOException{
        get_properties properties = new get_properties();
        Properties props = new Properties();
        props.put(ProducerConfig.ACKS_CONFIG, "1" );
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

}
