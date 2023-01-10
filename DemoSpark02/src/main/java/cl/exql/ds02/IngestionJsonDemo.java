package cl.exql.ds02;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class IngestionJsonDemo {

    public static void main(String[] args) {
        IngestionJsonDemo ingestionCsvDemo = new IngestionJsonDemo();
        ingestionCsvDemo.start();
    }

    public void start() {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Restaurant in Wake County, NC")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read().format("json")
                .load("data/Restaurants_in_Durham_County_NC.csv");
        System.out.println("*** After ingestion:");
        df.printSchema();


}
}
