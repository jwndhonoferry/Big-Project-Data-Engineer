package Consumer;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;

import Properties.*;
import Crawl_inform.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.LoggerFactory;

public class tweet_consumer {

    public static RestHighLevelClient createClient(){
        //10.184.0.7
        RestClientBuilder build = RestClient.builder(
                new HttpHost("localhost",9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(build);
        return client;
    }

    public static void runConsumer() throws IOException {
        final Consumer<Long, String> consumer = all_props.createConsumer(conf.TOPIC);

//        System.setProperty("es.set.netty.runtime.available.processors", "false");
//        Settings settings = Settings.builder().put("cluster.name", "my-app").build();

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            int rec_count = consumerRecords.count();

            for(ConsumerRecord<Long, String> record : consumerRecords){
                try {
                    RestHighLevelClient client = createClient();
                    IndexRequest ir = new IndexRequest("tweet_spotify_new", "_doc")
                            .source(record.value(), XContentType.JSON);
                    IndexResponse indexResponse = client.index(ir, RequestOptions.DEFAULT);

                    System.out.printf("Consumer Tweet Record:(%s, %d, %d)\n",
                            record.value(),
                            record.offset(),
                            record.partition());
                    client.close();
                } catch (Exception e) {
                    e.printStackTrace();
//                    log.warning("Skip data " + record.value());
                }
            }
            consumer.commitAsync();
        }

    }

    public static void main(String[] args) throws IOException {
        runConsumer();
    }
}
