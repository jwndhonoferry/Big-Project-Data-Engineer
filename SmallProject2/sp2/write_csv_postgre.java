package sp2;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.mongodb.spark.MongoSpark;

import static  org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class write_csv_postgre {
    private static Properties prop = new Properties();
    private static FileInputStream ip;
    static {
        try {
            ip = new FileInputStream("/home/jwndhono/Belajar Java/SmallProject2/src/main/resources/conn.properties");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {
        prop.load(ip);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Spark SQL Examples")
                .getOrCreate();

//        Dataset<Row> jdbcDF = spark.read()
//                .format("jdbc")
//                .option("url", prop.getProperty("db_url"))
//                .option("dbtable", "public.people")
//                .option("user", prop.getProperty("db_user"))
//                .option("password", prop.getProperty("db_pass"))
//                .load();
        Dataset<Row> df = spark.read()
                .option("mode", "DROPMALFORMED")
                .option("delimiter", ",")
                .option("header", "true")
//                .schema(schema)
                .option("inferSchema", true)
                .csv("/home/jwndhono/Belajar Java/SmallProject2/src/main/resources/data.csv");
        df.show();
        df.write()
                .mode("append")
                .format("jdbc")
                .option("url", prop.getProperty("db_url"))
                .option("dbtable","public.invoice")
                .option("user", prop.getProperty("db_user"))
                .option("password", prop.getProperty("db_pass"))
                .save();
    }

}
