package cl.exql.lab100_cv_to_db;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class CvsToRelationalDataBase {

    public static void main(String[] args) {
        CvsToRelationalDataBase cvsToRelationalDataBase = new CvsToRelationalDataBase();
        cvsToRelationalDataBase.start();
    }

    public void start() {
        //Obtener SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Transformar desde CSV a DB")
                .master("local")
                .getOrCreate();

        //Hacer la carga, ingesta, lectura.
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/authors.csv");

        //Transformar
        df = df.withColumn("name", concat(df.col("lname"), lit(", "), df.col("fname")));

        //Salvar datos
        String dbConnectionUrl = "jdbc:mysql://localhost/spark_labs";
        Properties prop = new Properties();
        prop.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        prop.setProperty("user", "root");
        prop.setProperty("password", "root");

        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "ch02e1_authors", prop);
    }

}
