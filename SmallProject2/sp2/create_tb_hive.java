package sp2;
import org.apache.log4j.*;
import org.apache.spark.sql.*;

//UDF
import static  org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
public class create_tb_hive {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("use small_project");
        spark.sql("CREATE TABLE invoice (InvoiceNo STRING, StockCode STRING, Description STRING, " +
                "Quantity INT, InvoiceDate STRING," +
                "UnitPrice Double, CustomerID STRING, Country STRING) " +
                "row format delimited fields terminated by ',' " +
                "tblproperties(\"skip.header.line.count\"=\"1\")");

        spark.sql("LOAD DATA LOCAL INPATH '/home/ferry18ixe/data_clean_fix.csv' " +
                "overwrite into table invoice");

//        Dataset<Row> soal1 = spark.sql("select StockCode, Description, max(Quantity) as maks from invoice \n" +
//                "group by StockCode, Description, Quantity order by Quantity desc");
//        soal1.show();
//        soal1.write().mode("overwrite").format("orc").saveAsTable("small_project.soal1_2");
    }
}
