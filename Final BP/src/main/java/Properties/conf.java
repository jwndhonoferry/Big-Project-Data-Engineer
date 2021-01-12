package Properties;

public class conf {
    //Consumer
    public final static String TOPIC = "tweet_spotify";
    //Kafka Server
    public final static String BOOTSTRAP_SERVERS = "localhost:9092";
    public final static String GROUP_ID_C = "group1";
    public final static String GROUP_ID_SS = "group2";
    public final static String MAX_POLL = "1";


    //Consumer elasticsearch
    public final static String ELASTIC = "10.184.0.7";



    //Kafka Spark Stream
    public final static String AUTO_OFSET = "latest";
    public final static boolean AUTO_COMIT = true;
}
