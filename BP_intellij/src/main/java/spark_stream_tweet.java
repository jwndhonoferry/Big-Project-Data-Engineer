import org.apache.spark.SparkConf$;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Time;
import org.mortbay.util.ajax.JSON;
import scala.Tuple2;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import javax.xml.crypto.Data;
import java.util.*;

public class spark_stream_tweet {
    public static void runSparkStream() throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("a").setMaster("local[*]");
        //batch duration
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));


        Map<String, Object> kafkaParams = new HashMap<>();
        // Ganti ini
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "Group1");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList("tweet_spotify");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> full_json = stream.map(record -> record.value());
//        full_json.print();
        full_json.foreachRDD(
                new VoidFunction2<JavaRDD<String>, Time>() {
                    @Override
                    public void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {
                        SparkSession spark = SparkSession.builder()
                                .config(stringJavaRDD.context().getConf())
                                .getOrCreate();
                        JavaRDD<JavaRow> rowRDD = stringJavaRDD.map(new Function<String, JavaRow>() {
                            public JavaRow call(String str) {
                                JavaRow record = new JavaRow();
                                crawl_url cr = new crawl_url();

                                JsonElement je = new JsonParser().parse(str);
                                JsonObject jo = je.getAsJsonObject();
                                String url_spot = jo.getAsJsonObject("entities")
                                        .getAsJsonArray("urls")
                                        .get(0).getAsJsonObject()
                                        .get("expanded_url").getAsString();
                                String usr = jo.getAsJsonObject("user").get("name").toString();
//                                System.out.println(usr);
                                JSONObject url = cr.get_information_spotify(url_spot);
                                url.put("user", usr);
                                String url_fix = url.toString();
//                                System.out.println(url_fix);
                                record.setWord(url_fix);
                                return record;
                            }
                        });
                        Dataset<Row> data = spark.createDataFrame(rowRDD, JavaRow.class);
//                        data.show();
                        List<String> a = data.as(Encoders.STRING()).collectAsList();
                        Dataset<Row> dj = spark.createDataset(a, Encoders.STRING())
                                .toDF()
                                .withColumnRenamed("full_title", "full_title");

                        Dataset<String> df1 = dj.as(Encoders.STRING());
                        Dataset<Row> df2 = spark.read().json(df1.javaRDD());
                        df2.show();
//                       To HDFS
//                        df2.coalesce(1).write()
//                                .format("csv")
//                                .option("header",true)
//                                .mode("append")
//                                .save("/tweet_spotify");
                    }
                });
//        full_json.print();
//        System.out.println("Full Data");
//        JavaDStream<String> url = stream.map(new song());
//        url.print();
//        System.out.println("Song Title");
//        JavaDStream<String> title = stream.map(new get_field("full_title"));
//        title.print();
//        System.out.println("Artist");
//        JavaDStream<String> artist = stream.map(new get_field("artist"));
//        artist.print();
//        System.out.println("Type");
//        JavaDStream<String> type = stream.map(new get_field("type"));
//        type.print();
        ssc.start();
        ssc.awaitTermination();
    }

    public static void main(String[] args) throws InterruptedException {
        runSparkStream();
    }
}

class song implements Function<ConsumerRecord<String, String>, String>{
    public String call(ConsumerRecord<String, String> cr) {
        crawl_url cu = new crawl_url();
        JsonElement je = new JsonParser().parse(cr.value());
        JsonObject jo = je.getAsJsonObject();
        String url_spot = jo.getAsJsonObject("entities").getAsJsonArray("urls").get(0).getAsJsonObject().get("expanded_url").getAsString();
        JSONObject url = cu.get_information_spotify(url_spot);
        String url_fix = url.toString();
        return url_fix;
    }
}
class consumer_data implements Function<ConsumerRecord<String, String>,String>{
    public String call(ConsumerRecord<String, String> cr) {
        return cr.value();
        }
}
class get_field implements Function<ConsumerRecord<String, String>,String>{
    private String x;
    public get_field(String field) {this.x = field;};
    public String call(ConsumerRecord<String, String> cr) {
        crawl_url cu = new crawl_url();
        JsonElement je = new JsonParser().parse(cr.value());
        JsonObject jo = je.getAsJsonObject();
        String url_spot = jo.getAsJsonObject("entities").getAsJsonArray("urls").get(0).getAsJsonObject().get("expanded_url").getAsString();
        JSONObject url = cu.get_information_spotify(url_spot);
        String fields = url.getString(x);
        return fields;
    }
}