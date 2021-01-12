import com.google.common.collect.Lists;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class tweet_producer_new {
    public static void runProducer(String consumerKey, String consumerSecret,
                            String token, String secret, String term) {

        BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Lists.newArrayList("twitterapi",term));

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
                JsonElement je = new JsonParser().parse(msg);
                JsonObject jo = je.getAsJsonObject();
                ProducerRecord<Long, String> message = new ProducerRecord<>(conf.TOPIC, msg);
                if(jo.getAsJsonObject("entities").getAsJsonArray("urls").size() > 0 ){
                    String url_spotify = jo.getAsJsonObject("entities")
                            .getAsJsonArray("urls")
                            .get(0).getAsJsonObject()
                            .get("expanded_url")
                            .getAsString();
//                    System.out.println(url_spotify);
                    Pattern pattern = Pattern.compile("\\bopen.spotify.com\\b");
                    Matcher match = pattern.matcher(url_spotify);

                    CharSequence key = "open.spotify.com";
                    boolean bool = url_spotify.contains(key);
                    if(match.find() && bool==true){
//                        System.out.println(bool==true);
                        System.out.println(message);
                        producer.send(message);
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }
    }

    private static Producer<Long, String> getProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        tweet_producer_new.runProducer(conf.consumer_key,
                conf.consumer_secret,
                conf.access_token,
                conf.access_token_secret,
                conf.words_search_key);
    }
}
