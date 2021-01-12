import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;

public class main_Thread extends Thread{
    private static Properties prop = new Properties();
    private static FileInputStream ip;
    static {
        try {
            ip = new FileInputStream("/home/jwndhono/Data Engineer/Belajar Java/BP_intellij/src/main/resources/key.properties");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void run(){
        try {
            prop.load(ip);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String search = "spotify";

        try {
            tweet_producer_new.runProducer(prop.getProperty("consumer_key"),
                    prop.getProperty("consumer_secret"),
                    prop.getProperty("access_token"),
                    prop.getProperty("access_token_secret"),
                    search);
            tweet_consumer_kafka.runConsumer();
            spark_stream_tweet.runSparkStream();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
