package net.jgp.books.spark.ch07.lab700_text_ingestion;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.expressions.Window.orderBy;
import static org.apache.spark.sql.expressions.Window.rowsBetween;
import static org.apache.spark.sql.functions.*;

/**
 * Text ingestion in a dataframe.
 * <p>
 * Source of file: Rome & Juliet (Shakespeare) -
 * http://www.gutenberg.org/cache/epub/1777/pg1777.txt
 *
 * @author jgp
 */
public class TextToDataframeApp {

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        TextToDataframeApp app = new TextToDataframeApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Text to Dataframe")
                .master("local")
                .getOrCreate();

        // Reads a Romeo and Juliet (faster than you!), stores it in a dataframe
        Dataset<Row> df = spark
                .read()
                .format("text")
                .load("data/romeo-juliet-pg1777.txt");

        long rowCount = df.count();
        int counter = 0;

        // Filter to start at line 295 displaying "ACT I. Scene I." This means the zero based index column > 293
        FilterFunction<Row> startAfterRow = record -> (Long)record.get(1) > 293;

        //creating a new dataframe with an index column containing the line_number - 1 (starts counting at 0)
        Dataset<Row> indexed_df = df.withColumn("line_number", monotonically_increasing_id());
        indexed_df
                .filter(startAfterRow)
                .show(90, 500);
        indexed_df.printSchema();
    }
}
