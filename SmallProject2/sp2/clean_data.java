package sp2;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import static org.apache.spark.sql.functions.max;

public class clean_data {
    private static Properties prop = new Properties();
    private static FileInputStream ip;
    static {
        try {
            ip = new FileInputStream("/home/jwndhono/Belajar Java/SmallProject2/src/main/resources/conn.properties");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public final SparkSession spark;

    public clean_data() {
        super();
        spark = SparkSession
                .builder()
                .appName("MainCLass").master("local").getOrCreate();
    }

    public static void main(String[] args) throws IOException {
        prop.load(ip);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        clean_data obj_main = new clean_data();

        Dataset<Row> data = obj_main.spark.read()
                .format("jdbc")
                .option("url", prop.getProperty("db_url"))
                .option("user", prop.getProperty("db_user"))
                .option("password", prop.getProperty("db_pass"))
                .option("dbtable", "public.invoice")
                .load();
        data.show();

        data.registerTempTable("invoice");

//        data.write().option("header", "true").csv("/home/jwndhono/Belajar Java/SmallProject2/src/main/resources/data_clean.csv");

        Dataset<Row> create_tb_desc = obj_main.spark.sql("select Description, " +
                "sum(Quantity) as maks from invoice \n" +
                "group by Description order by maks desc");
//        create_tb_desc.show();

        Dataset<Row> create_tb_stock = obj_main.spark.sql("select StockCode, " +
                "sum(Quantity) as maks from invoice \n" +
                "group by StockCode order by maks desc\n");
        create_tb_stock.show();

        Dataset<Row> hasil_stock_max = obj_main.spark.sql("select invoice.StockCode, invoice.Description," +
                "invoice.Quantity ,invoice.InvoiceDate, invoice.Country \n" +
                "from invoice \n" +
                "join (select invoice.StockCode, sum(invoice.Quantity) as maks from invoice\n" +
                "group by invoice.StockCode order by maks desc limit 1) as tb_1\n" +
                "on (tb_1.StockCode = invoice.StockCode) \n" +
                "join (select Country, count(StockCode) as coun from invoice group by Country ) as tb_con\n" +
                "on (tb_con.Country = invoice.Country )\n" +
                "order by Quantity desc limit 1");

        Dataset<Row> hasil_stock_min = obj_main.spark.sql("select invoice.StockCode, " +
                "invoice.Description, " +
                "invoice.Quantity , invoice.InvoiceDate, invoice.Country \n" +
                "from invoice \n" +
                "join (select invoice.StockCode, sum(invoice.Quantity) as maks from invoice\n" +
                "group by invoice.StockCode order by maks asc limit 1) as tb_1\n" +
                "on tb_1.StockCode = invoice.StockCode\n" +
                "join (select Country, count(StockCode) as coun from invoice group by Country ) as tb_con\n" +
                "on tb_con.Country = invoice.Country\n" +
                "order by Quantity asc limit 1");
        Dataset<Row> result_stock = hasil_stock_max.unionAll(hasil_stock_min);
//        result_stock.show();


    }
}

