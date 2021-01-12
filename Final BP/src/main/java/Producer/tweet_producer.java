package Producer;

import Properties.conf;
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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import Properties.all_props;
import Properties.get_properties;

public class tweet_producer {
    public static void runProducer(String consumerKey, String consumerSecret,
                                   String token, String secret, String term){

        BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        endpoint.trackTerms(Lists.newArrayList("twitterapi",term));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

        Client client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();
        client.connect();

        try (Producer<Long, String> producer = all_props.createProducer() ) {
            while (true) {

                String msg = queue.take();
                JsonElement je = new JsonParser().parse(msg);
                JsonObject jo = je.getAsJsonObject();
                ProducerRecord<Long, String> message = new ProducerRecord<>(conf.TOPIC, msg);

                // Check msg, if they have url contains open.spotify.com then get it
                if(jo.getAsJsonObject("entities").getAsJsonArray("urls").size() > 0 ){
                    String url_spotify = jo.getAsJsonObject("entities")
                            .getAsJsonArray("urls")
                            .get(0).getAsJsonObject()
                            .get("expanded_url")
                            .getAsString();
//                  Regex pattern
                    Pattern pattern = Pattern.compile("\\bopen.spotify.com\\b");
                    Matcher match = pattern.matcher(url_spotify);
//                  Second check
                    CharSequence key = "open.spotify.com";
                    boolean bool = url_spotify.contains(key);
                    if(match.find() && bool==true){
                        System.out.println(message);
                        producer.send(message);
                    }
                }
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }
    }
    public static void main(String[] args) throws IOException {
        Properties prop = new get_properties().read();
        tweet_producer.runProducer(
                prop.getProperty("consumer_key"),
                prop.getProperty("consumer_secret"),
                prop.getProperty("access_token"),
                prop.getProperty("access_token_secret"),
                prop.getProperty("words_search_key"));
    }
}
