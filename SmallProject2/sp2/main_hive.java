package sp2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.io.File;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

//UDF


public class main_hive {
    public final SparkSession spark;

    public main_hive() {
        super();
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

//        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
//        SparkSession spark = SparkSession.builder()
//                .appName("Java Spark Hive Example")
//                .config("spark.sql.warehouse.dir", warehouseLocation)
//                .enableHiveSupport()
//                .getOrCreate();

        main_hive obj_main = new main_hive();
        Dataset<Row> data = obj_main.spark.sql("select * from small_project.invoice");

        data.registerTempTable("invoice");

        Dataset<Row> create_tb_desc = obj_main.spark.sql("select Description, sum(Quantity) as maks from invoice \n" +
                "group by Description order by maks desc");
//        create_tb_desc.show();

        Dataset<Row> create_tb_stock = obj_main.spark.sql("select StockCode, sum(Quantity) as maks from invoice\n" +
                "group by StockCode order by maks desc");
//        create_tb_stock.show();

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
                "on (tb_1.StockCode = invoice.StockCode)\n" +
                "join (select Country, count(StockCode) as coun from invoice group by Country ) as tb_con\n" +
                "on (tb_con.Country = invoice.Country)\n" +
                "order by Quantity asc limit 1");

        Dataset<Row> result_stock = hasil_stock_max.unionAll(hasil_stock_min);
//        result_stock.show();
//        result_stock.write().mode("overwrite").format("hive").saveAsTable("small_project.result_stock");

        // StockCode
        Dataset<Row> sum_tot = data.groupBy("StockCode")
                .agg(sum("Quantity"))
                .select(col("StockCode").as("col_sc"),
                        col("sum(Quantity)").as("sum_qty"))
                .sort(desc("sum_qty")).limit(1);
        sum_tot.show();

        Dataset<Row> maks_country = data.groupBy("Country")
                .agg(count("StockCode"))
                .select(col("Country").as("col_country"),
                        col("count(StockCode)").as("coun"));
        maks_country.show();

        Dataset<Row> result_stock_max = data.join(sum_tot, data.col("StockCode")
                .equalTo(sum_tot.col("col_sc")))
                .join(maks_country, data.col("Country")
                        .equalTo(maks_country.col("col_country")))
                .select(col("StockCode"),
                        col("Description"),
                        col("InvoiceDate"),
                        col("Quantity"),
                        col("Country"))
                .sort(desc("Quantity")).limit(1);
        result_stock_max.show();
// Min
        Dataset<Row> sum_tot_asc = data.groupBy("StockCode")
                .agg(sum("Quantity"))
                .select(col("StockCode").as("col_sc"),
                        col("sum(Quantity)").as("sum_qty"))
                .sort(asc("sum_qty")).limit(1);

        Dataset<Row> result_stock_min = data.join(sum_tot, data.col("StockCode")
                .equalTo(sum_tot_asc.col("col_sc")))
                .join(maks_country, data.col("Country")
                        .equalTo(maks_country.col("col_country")))
                .select(col("StockCode"),
                        col("Description"),
                        col("InvoiceDate"),
                        col("Quantity"),
                        col("Country"))
                .sort(asc("Quantity")).limit(1);
        result_stock_min.show();

        Dataset<Row> result_stockcode = result_stock_max.unionAll(result_stock_min);
        result_stockcode.show();

        result_stockcode.write()
                .mode("overwrite")
                .format("hive")
                .saveAsTable("small_project.result_stock");

        //Description
//        Dataset<Row> sum_tot_desc = data.groupBy("Description")
//                .agg(sum("Quantity"))
//                .select(col("Description").as("col_desc"),
//                        col("sum(Quantity)").as("sum_qty_desc"))
//                .sort(desc("sum_qty_desc")).limit(1);
//        sum_tot.show();
//
//        Dataset<Row> maks_country_desc = data.groupBy("Country")
//                .agg(count("StockCode"))
//                .select(col("Country").as("col_country_desc"),
//                        col("count(StockCode)").as("coun_desc"));
//        maks_country.show();
//
//        Dataset<Row> result_desc_max = data.join(sum_tot_desc, data.col("StockCode")
//                .equalTo(sum_tot_desc.col("col_desc")))
//                .join(maks_country, data.col("Country")
//                        .equalTo(maks_country.col("col_country_desc")))
//                .select(col("StockCode"),
//                        col("Description"),
//                        col("InvoiceDate"),
//                        col("Quantity"),
//                        col("Country"))
//                .sort(desc("Quantity")).limit(1);
//        result_desc_max.show();
////        Min
//        Dataset<Row> sum_tot_desc_asc = data.groupBy("Description")
//                .agg(sum("Quantity"))
//                .select(col("Description").as("col_asc"),
//                        col("sum(Quantity)").as("sum_qty_desc"))
//                .sort(asc("sum_qty_desc")).limit(1);
//        sum_tot.show();
//        Dataset<Row> result_desc_min = data.join(sum_tot_desc_asc, data.col("StockCode")
//                .equalTo(sum_tot_desc_asc.col("col_asc")))
//                .join(maks_country, data.col("Country")
//                        .equalTo(maks_country.col("col_country_desc")))
//                .select(col("StockCode"),
//                        col("Description"),
//                        col("InvoiceDate"),
//                        col("Quantity"),
//                        col("Country"))
//                .sort(asc("Quantity")).limit(1);
//        result_desc_min.show();
    }
//    private void write_tb_desc(Dataset<Row> data){
//
//        Dataset<Row> soal1_2 = obj_main.spark.sql();
//        soal1_2.write().mode("overwrite").format("orc").saveAsTable("default.rerata_penambahan_kasus");
//    }
}
