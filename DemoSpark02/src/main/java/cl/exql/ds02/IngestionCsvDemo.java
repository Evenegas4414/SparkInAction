package cl.exql.ds02;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
public class IngestionCsvDemo {

    public static void main(String[] args) {
        IngestionCsvDemo ingestionCsvDemo = new IngestionCsvDemo();
        ingestionCsvDemo.start();
    }

    public void start() {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Restaurant in Wake County, NC")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read().format("csv")
                .option("header", "true")
                .load("data/Restaurants_in_Wake_County_NC.csv");
        System.out.println("*** After ingestion:");
        df.show(5);
        df.printSchema();
        System.out.println("Records: " + df.count());

        df = df.withColumn("county", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address1")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop("OBJECTID")
                .drop("PERMITID")
                .drop("GEOCODESTATUS");

        System.out.println("*** After Transformation1:");
        df.show(5);
        df.printSchema();
        System.out.println("Records: " + df.count());

        df = df.withColumn("id", concat(
                df.col("state"), lit("_"),
                df.col("county"), lit("_"),
                df.col("datasetId")));

        System.out.println("*** After Transformation2:");
        df.show(5);
        df.printSchema();
        System.out.println("Records: " + df.count());

        System.out.println("*** Looking at partitions");
        Partition[] partitions =  df.rdd().partitions();
        System.out.println(partitions);
        System.out.println("Partition count before repartition: " + partitions.length);

        df = df.repartition(3);
        System.out.println("Partition count after repartition: " + df.rdd().partitions().length);

        StructType schema = df.schema();

        System.out.println("*** Schema as a tree:");
        schema.printTreeString();
        String schemaAsString = schema.mkString();
        System.out.println("*** Schema as string: " + schemaAsString);
        String schemaAsJson = schema.prettyJson();
        System.out.println("*** Schema as JSON: " + schemaAsJson);
    }
}
