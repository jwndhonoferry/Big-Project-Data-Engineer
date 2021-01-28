package consumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class tweet_consumer_kafka {
    private final static String TOPIC = "tweet_spotify";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    static void runConsumer() throws InterruptedException, UnknownHostException {
        final Consumer<Long, String> consumer = createConsumer();
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        Settings settings = Settings.builder().put("cluster.name", "elastic-cluster").build();
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

        final int batas = 100;
        int noRecordsCount = 0;

        while (true) {
            // mengembalikan record besarkan offset partisi syg digunakan(saat ini).
            //Metode polling adalah metode pemblokiran yang menunggu waktu tertentu dalam hitungan detik.
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(Duration.ofMillis(1000));

            // batasan,
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > batas) break;
                else continue;
            }
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Tweet Record:(%s, %d, %d)\n",
                        record.value(),
                        record.partition(),
                        record.offset());
                IndexResponse response = client.prepareIndex("tweet_spotify_new","_doc").setSource(record.value(), XContentType.JSON).get();
            });
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }

    public static void main(String[] args) throws InterruptedException, UnknownHostException {
        runConsumer();
    }
}
