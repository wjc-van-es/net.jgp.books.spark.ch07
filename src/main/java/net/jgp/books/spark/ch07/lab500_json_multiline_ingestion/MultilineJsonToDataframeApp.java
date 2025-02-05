package net.jgp.books.spark.ch07.lab500_json_multiline_ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Multiline ingestion JSON ingestion in a dataframe.
 *
 * @author jgp
 */
public class MultilineJsonToDataframeApp {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        MultilineJsonToDataframeApp app =
                new MultilineJsonToDataframeApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Multiline JSON to Dataframe")
                .master("local")
                .getOrCreate();

        // Reads a JSON, called countrytravelinfo.json, stores it in a dataframe
        Dataset<Row> df = spark.read()
                .format("json")
                .option("multiline", true) // is essential when dealing with complex formatted json instead of jsonlines
                .load("data/countrytravelinfo.json");

        // Shows at most 3 rows from the dataframe
        df.show(15, 20);
        df.printSchema();
    }
}
