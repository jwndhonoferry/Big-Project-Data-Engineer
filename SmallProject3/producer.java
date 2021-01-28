package consumer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class producer {
    private static void run(String consumerKey, String consumerSecret,
                            String token, String secret, String term) {

        BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Lists.newArrayList(
                term));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
                secret);

        Client client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();


        client.connect();
        try (Producer<Long, String> producer = getProducer()) {
            while (true) {
                String msg = queue.take();
                System.out.println(msg);
                ProducerRecord<Long, String> message = new ProducerRecord<>(producer_conf.TOPIC, queue.take());
                producer.send(message);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }
    }

    private static Producer<Long, String> getProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producer_conf.SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        String consumer_key = "rPVgcSXlmHSmH9nuRMz7Hugh9";
        String consumer_secret = "fQWTsz1n5KxDJJ9fatNMxFeFxnSm1YmPM706EyW7ac5yrTsinF";
        String access_token = "2401826684-ltRNlBUfHbyD3irdwXRpPotlv6e2cqCNdVFX9ld";
        String access_token_secret = "izoNEtlVg5LSBwIHKtlMvLNV6qlkLB79gNzDhqB9OTfG9";
        String words_search_key = "#spotify";
        producer.run(consumer_key, consumer_secret, access_token, access_token_secret, words_search_key);

    }

}
