//package consumer;
//
//import com.google.common.collect.Lists;
//import com.twitter.hbc.ClientBuilder;
//import com.twitter.hbc.core.Client;
//import com.twitter.hbc.core.Constants;
//import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
//import com.twitter.hbc.core.processor.StringDelimitedProcessor;
//import com.twitter.hbc.httpclient.auth.Authentication;
//import com.twitter.hbc.httpclient.auth.OAuth1;
//import kafka.producer.KeyedMessage;
//import kafka.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.Producer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.LongSerializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//
//import javax.ws.rs.core.Response.Status;
//import java.util.Properties;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.LinkedBlockingQueue;
//
//public class tweet_producer_kafka {
//    public static final String name_topic = "tweet_spotify";
//
//    public static void TweetMessage(Producer<String, String> producer) throws InterruptedException {
//        String consumer_key = "rPVgcSXlmHSmH9nuRMz7Hugh9";
//        String consumer_secret = "fQWTsz1n5KxDJJ9fatNMxFeFxnSm1YmPM706EyW7ac5yrTsinF";
//        String access_token = "2401826684-ltRNlBUfHbyD3irdwXRpPotlv6e2cqCNdVFX9ld";
//        String access_token_secret = "izoNEtlVg5LSBwIHKtlMvLNV6qlkLB79gNzDhqB9OTfG9";
//
//        KeyedMessage<String, String> message = null;
//
//        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
//        StatusesFilterEndpoint endpoint =  new StatusesFilterEndpoint();
//
//        endpoint.trackTerms(Lists.newArrayList("twitterapi", "#open.spotify.com"));
//
//        Authentication auth =  new OAuth1(consumer_key, consumer_secret,
//                access_token, access_token_secret);
//
//
//        Client client = new ClientBuilder()
//                .hosts(Constants.STREAM_HOST)
//                .endpoint(endpoint)
//                .authentication(auth)
//                .processor(new StringDelimitedProcessor(queue))
//                .build();
//
//        client.connect();
//
//        for (int msg_read = 0; msg_read < 1000; msg_read++) {
//            try {
//                String msg = queue.take();
//                System.out.println(msg);
//                message = new KeyedMessage<String, String>(name_topic, queue.take());
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            producer.send(message);
//        }
//        producer.close();
//        client.stop();
//
//    }
//
//    public static void main(String[] args) {
//        Properties props = new Properties();
//
//        props.put("metadata.broker.list", "localhost:9092");
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//
//        ProducerConfig prod_config =  new ProducerConfig(props);
//        Producer<String, String> producer = new Producer<String, String>(prod_config);
//        try{
//            tweet_producer_kafka.TweetMessage(producer);
//
//        } catch (InterruptedException e) {
//            System.out.println(e);
//        }
//
//
//    }
//}
//
