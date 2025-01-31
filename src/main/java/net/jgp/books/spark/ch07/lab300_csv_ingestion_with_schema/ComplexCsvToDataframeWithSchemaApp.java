package net.jgp.books.spark.ch07.lab300_csv_ingestion_with_schema;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import net.jgp.books.spark.ch07.utils.SchemaInspector;

/**
 * CSV ingestion in a dataframe with a Schema.
 *
 * @author jgp
 */
public class ComplexCsvToDataframeWithSchemaApp {

    public static final DecimalType$ DecimalType = DecimalType$.MODULE$;

    /**
     * main() is your entry point to the application.
     *
     * @param args
     */
    public static void main(String[] args) {
        ComplexCsvToDataframeWithSchemaApp app =
                new ComplexCsvToDataframeWithSchemaApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Complex CSV with a schema to Dataframe")
                .master("local")
                .getOrCreate();

        // Creates the schema using
        // org.apache.spark.sql.types.DataTypes.createStructType(org.apache.spark.sql.types.StructField[])
        StructType schema = DataTypes.createStructType(new StructField[]{
                // using org.apache.spark.sql.types.DataTypes.createStructField(
                // java.lang.String, //name
                // org.apache.spark.sql.types.DataType, // type
                // boolean) //nullable
                DataTypes.createStructField(
                        "id",
                        DataTypes.IntegerType,
                        false),
                DataTypes.createStructField(
                        "authordId",
                        DataTypes.IntegerType,
                        true),
                DataTypes.createStructField(
                        "bookTitle",
                        DataTypes.StringType,
                        false),
                DataTypes.createStructField(
                        "releaseDate",
                        DataTypes.DateType,
                        true), // nullable, but this will be ignore
                DataTypes.createStructField(
                        "url",
                        DataTypes.StringType,
                        false)});

        // GitHub version only: dumps the schema
        // version without additional label (which is used further below instead)
        SchemaInspector.print(schema);

        // Reads a CSV file with header, called books.csv, stores it in a
        // dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("dateFormat", "MM/dd/yyyy")
                .option("quote", "*")
                .schema(schema)
                .load("data/books.csv");

        // GitHub version only: dumps the schema
        // You compare the schema of schema with the schema applied to dataframe df and you'll see no differences
        // obviously
        SchemaInspector.print("Schema ......\n ", schema);
        SchemaInspector.print("Dataframe ... \n", df);

        // Shows at most 30 rows from the dataframe
        df.show(30, 180, false);
        df.printSchema();
    }
}
