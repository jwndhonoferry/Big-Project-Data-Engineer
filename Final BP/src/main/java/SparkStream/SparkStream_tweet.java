package SparkStream;

import Consumer.tweet_consumer;
import Producer.tweet_producer;
import org.apache.http.HttpHost;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Time;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bouncycastle.asn1.dvcs.Data;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import Crawl_inform.*;
import Properties.*;

public class SparkStream_tweet {

    public static void runSparkStream() throws InterruptedException, IOException {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("a")
//                .set("spark.driver.allowMultipleContexts", "true")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //batch duration
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));
        Collection<String> topics = Arrays.asList("tweet_spotify");
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, all_props.kafkaParams())
                );

        JavaDStream<String> full_json = stream.map(new consumer_data());
            full_json.foreachRDD(
                    new VoidFunction2<JavaRDD<String>, Time>() {
                        @Override
                        public void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {
                            SparkSession spark = SparkSession.builder()
                                    .config(stringJavaRDD.context().getConf())
                                    .getOrCreate();
                            JavaRDD<JavaRow> rowRDD = stringJavaRDD.map(new Function<String, JavaRow>() {
                                public JavaRow call(String str) throws IOException {
                                    JavaRow record = new JavaRow();
                                    crawl_url cr = new crawl_url();

                                    JsonElement je = new JsonParser().parse(str);
                                    JsonObject jo = je.getAsJsonObject();
                                    String url_spot = jo.getAsJsonObject("entities")
                                            .getAsJsonArray("urls")
                                            .get(0).getAsJsonObject()
                                            .get("expanded_url").getAsString();
                                    String usr = jo.getAsJsonObject("user").get("name").toString();
                                    JSONObject url = cr.get_information_spotify(url_spot);
                                    url.put("user", usr);
                                    String url_fix = url.toString();
                                    record.setWord(url_fix);
                                    return record;
                                }
                            });
                            Dataset<Row> data = spark.createDataFrame(rowRDD, JavaRow.class);
                            List<String> a = data.as(Encoders.STRING()).collectAsList();
                            Dataset<Row> dj = spark.createDataset(a, Encoders.STRING())
                                    .toDF()
                                    .withColumnRenamed("full_title", "full_title");
                            Dataset<String> df1 = dj.as(Encoders.STRING());

                            Dataset<Row> df2 = spark.read().json(df1.javaRDD());
//                            Dataset<Row> song = df2.select(df2.col("*")).where(df2.col("type").equalTo("music.song"));
//                            Dataset<Row> album = df2.select(df2.col("*")).where(df2.col("type").equalTo("music.album"));
//                            Dataset<Row> playlist = df2.select(df2.col("*")).where(df2.col("type").equalTo("music.playlist"));
//                            Dataset<Row> website = df2.select(df2.col("*")).where(df2.col("type").equalTo("website"));
//                            Song.show();
                            df2.show();
//                            send_hdfs(df2, "/data_all.csv","csv");
                        }
                    });
//            full_json.foreachRDD(rdd -> new passRDD().call(rdd, sc));
        ssc.start();
        ssc.awaitTermination();
    }
    public static void data_classification(Dataset<Row> data) throws IOException {
        Dataset<Row> song = data.select(data.col("*")).where(data.col("type").equalTo("music.song"));
        Dataset<Row> album = data.select(data.col("*")).where(data.col("type").equalTo("music.album"));
        Dataset<Row> playlist = data.select(data.col("*")).where(data.col("type").equalTo("music.playlist"));
        Dataset<Row> website = data.select(data.col("*")).where(data.col("type").equalTo("website"));
//        song.show();
        send_hdfs(song, "/data_song.csv","csv");
        send_hdfs(album, "/data_album.csv","csv");
        send_hdfs(playlist, "/data_playlist.csv","csv");
        send_hdfs(website, "/data_website.csv","csv");
    }
    public static void send_hdfs(Dataset<Row> ds, String FolderName, String typeFile) {
        try {
            if(ds.count() != 0) {
                if(typeFile.equals("csv")) {
                    ds.write().format("csv")
                            .option("header", "true")
                            .mode("append")
                            .option("sep",";")
                            .save(FolderName);
                }
                else if(typeFile.equals("json")) {
                    ds.write().mode("append").json(FolderName);
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws InterruptedException, IOException {
        runSparkStream();
//        Properties prop = new get_properties().read();
//        Thread tp = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    tweet_producer.runProducer(
//                prop.getProperty("consumer_key"),
//                prop.getProperty("consumer_secret"),
//                prop.getProperty("access_token"),
//                prop.getProperty("access_token_secret"),
//                prop.getProperty("words_search_key"));
//                    System.out.println("Producer Run");
//                } catch(Exception e){
//                    e.printStackTrace();
//                }
//            }
//        });
//        Thread tc =  new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    tweet_consumer.runConsumer();
//                    System.out.println("Consumer Run");
//                }catch (Exception e){
//                    e.printStackTrace();
//                }
//            }
//        });
//
//        Thread ss = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try{
//                    runSparkStream();
//                    System.out.println("Spark Stream Run");
//                }catch (Exception e){
//                    e.printStackTrace();
//                }
//            }
//        });
//        tc.start();
//        ss.start();
//        tp.start();
//
//
//        tc.join();
//        ss.join();
//        tp.join();

    }
}
//class data_row{
//    public void call(JavaRDD<String> stringJavaRDD, JavaSparkContext sc) throws Exception {
//        List<String> list = stringJavaRDD.collect();
//        crawl_url cr = new crawl_url();
//        SparkSession spark = JavaSparkSessionSingleton.getInstance(stringJavaRDD.context().getConf());
//        if(!stringJavaRDD.isEmpty()){
//            try{
//                List<String> temp = new ArrayList<>();
//                list.forEach(record -> temp.add());
//            } catch (Exception e){
//                e.printStackTrace();
//            }
//        }
//
//    }
//}
class consumer_data implements Function<ConsumerRecord<String, String>,String>{
    public String call(ConsumerRecord<String, String> cr) {
        return cr.value();
    }
}
class get_song{
    public static String getJSONInformation(String msg) {

        crawl_url getData = new crawl_url();
        JSONObject j_obj = new JSONObject();
        try {
            JsonElement jsonElement = new JsonParser().parse(msg);
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            String spotify_url = jsonObject.getAsJsonObject("entities")
                    .getAsJsonArray("urls")
                    .get(0).getAsJsonObject()
                    .get("expanded_url")
                    .getAsString();
            String usr = jsonObject.getAsJsonObject("user")
                    .get("name")
                    .toString();
            j_obj = getData.get_information_spotify(spotify_url);
            j_obj.put("user", usr);

        }
        catch(Exception e){
            e.printStackTrace();
        }
        return j_obj.toString();
    }
}
class passRDD{
    public void call(JavaRDD<String> rdd,JavaSparkContext jsc ) throws Exception{
        List<String> list_rdd = rdd.collect();
        SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
        if(!rdd.isEmpty()) {
            try {
                JavaRDD<String> list_rdd_parallize = jsc.parallelize(list_rdd);
                Dataset<Row> full_data = spark.read().json(list_rdd_parallize);
                List<String> tempVariable = new ArrayList<>();
                list_rdd.forEach(msg -> tempVariable.add(get_song.getJSONInformation(msg)));
                JavaRDD<String> sample_data_spotifyRDD = jsc.parallelize(tempVariable);
                Dataset<Row> data_ = spark.read().json(sample_data_spotifyRDD);
                SparkStream_tweet.data_classification(data_);
                data_.show();
//		    spark.close();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
}
class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;
    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession
                    .builder()
                    .config(sparkConf)
                    .getOrCreate();
        }
        return instance;
    }
}
//class get_field implements Function<ConsumerRecord<String, String>,String>{
//    private String x;
//    public get_field(String field) {this.x = field;};
//    public String call(ConsumerRecord<String, String> cr) {
//        crawl_url cu = new crawl_url();
//        JsonElement je = new JsonParser().parse(cr.value());
//        JsonObject jo = je.getAsJsonObject();
//        String url_spot = jo.getAsJsonObject("entities").getAsJsonArray("urls").get(0).getAsJsonObject().get("expanded_url").getAsString();
//        JSONObject url = cu.get_information_spotify(url_spot);
//        String fields = url.getString(x);
//        return fields;
//    }
//}
//class song implements Function<ConsumerRecord<String, String>, String>{
//    public String call(ConsumerRecord<String, String> cr) {
//        crawl_url cu = new crawl_url();
//        JsonElement je = new JsonParser().parse(cr.value());
//        JsonObject jo = je.getAsJsonObject();
//        String url_spot = jo.getAsJsonObject("entities").getAsJsonArray("urls").get(0).getAsJsonObject().get("expanded_url").getAsString();
//        JSONObject url = cu.get_information_spotify(url_spot);
//        String url_fix = url.toString();
//        return url_fix;
//    }
//}
//class LoopAndPassingRDD{
//    public void call(JavaRDD<String> rdd, JavaSparkContext jsc ) throws Exception{
//
//        List<String> list_rdd = rdd.collect();
//        SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
//        if(!rdd.isEmpty()) {
//            try {
//                JavaRDD<String> list_rdd_parallize = jsc.parallelize(list_rdd);
//                Dataset<Row> data_mentah = spark.read().json(list_rdd_parallize);
//
//                List<String> tempVariable = new ArrayList<>();
//                list_rdd.forEach(msg -> tempVariable.add(getInformation.getJSONInformation(msg)));
//                JavaRDD<String> sample_data_spotifyRDD = jsc.parallelize(tempVariable);
//                Dataset<Row> data = spark.read().json(sample_data_spotifyRDD);
//                writeToStorage.writeData_structured(data);
//            }
//            catch(Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }
//}