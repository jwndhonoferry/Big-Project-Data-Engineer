package consumer;

public interface KafkaProducerAPI {
    public static String KAFKA_BROKERS = "0.0.0.0:9092";
    public static String ACKS = "all";
    public static String CLIENT_ID = "producer1";
    public static String TOPIC = "tweet_covid";
}
