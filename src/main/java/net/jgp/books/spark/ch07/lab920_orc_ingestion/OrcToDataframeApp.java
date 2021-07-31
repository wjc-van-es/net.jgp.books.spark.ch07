package net.jgp.books.spark.ch07.lab920_orc_ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * ORC ingestion in a dataframe.
 * <p>
 * Source of file: Apache ORC project -
 * https://github.com/apache/orc/tree/master/examples
 *
 * @author jgp
 */
public class OrcToDataframeApp {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        OrcToDataframeApp app = new OrcToDataframeApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("ORC to Dataframe")
                .config("spark.sql.orc.impl", "native") //configure using native rather than hive implementation
                .master("local")
                .getOrCreate();

        // Reads an ORC file, stores it in a dataframe
        Dataset<Row> df = spark.read()
                .format("orc") // specify the orc format
                .load("data/demo-11-zlib.orc");

        // Shows at most 10 rows from the dataframe
        df.show(10);
        df.printSchema();
        System.out.println("The dataframe has " + df.count() + " rows.");
    }
}
