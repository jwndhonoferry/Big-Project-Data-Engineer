
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

class spark_con{

}

public class tweet_consumer_kafka {
    private final static String TOPIC = "tweet_spotify";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "Group1");
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
        crawl_url crawl = new crawl_url();

        System.setProperty("es.set.netty.runtime.available.processors", "false");
        Settings settings = Settings.builder().put("cluster.name", "es-cluster").build();
        // Ganti localhost dengan alamat ipnya
        TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
//        RestHighLevelClient client = new RestHighLevelClient(
//                RestClient.builder(new HttpHost("localhost", 9200, "http")));
        final int batas = 100;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            // batasan,
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > batas) break;
                else continue;
            }
            consumerRecords.forEach(record -> {
//                crawl.get_information_spotify(record.value());
                System.out.printf("Consumer Tweet Record:(%s, %d, %d)\n",
                        record.value(),
                        record.partition(),
                        record.offset());
                IndexResponse response = client
                        .prepareIndex("tweet_spotify_new", "_doc")
                        .setSource(record.value(), XContentType.JSON).get();
//                 IndexRequest ir = new IndexRequest("tweet_spotify_new","_doc").source(record.value(), XContentType.JSON);
//                try {
//                    IndexResponse res = client.index(ir, RequestOptions.DEFAULT);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
            });
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE FELLAS");
    }

    public static void main(String[] args) throws InterruptedException, UnknownHostException {
        runConsumer();
    }
}
