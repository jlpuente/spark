package org.jlpuente.RDD;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * RDD-based Java Spark program that reads a CSV file,
 * which must be in the root directory of the project,
 * and counts the number of Spanish airports by type of airport.
 */

public class SpanishAirports {

    public static void main(String[] args) {

        // Step 0. Turn off the log messages
        Logger.getRootLogger().setLevel(Level.OFF);

        // Step 1. Create a SparkConf object
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("SpanishAirports");

        // Step 2. Create a Java Spark Context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Step 3. Point to the CSV file
        JavaRDD<String> lines = sparkContext.textFile("airports.csv");

        // Step 4. Read the header
        String header = lines.first();

        // Step 4.1. Store the indices and the Spanish ISO code
        int iso_country_index = 8;
        int name_index = 2;
        String spanish_ISO_code = "ES";

        // Step 5. Make the stream
        JavaPairRDD<Integer, String> spanishAiportsType = lines
                .filter((String s) -> !s.contains(header))
                .mapToPair(airport -> new Tuple2<>(airport.split(",")[iso_country_index],
                        airport.split(",")[name_index]))
                .filter(country -> country._1.contains(spanish_ISO_code))
                .mapToPair(pair -> new Tuple2<>(pair._2(), 1))
                .reduceByKey(Integer::sum)
                .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()))
                .sortByKey(false)
                .cache();

        // Step 6. Print the results on screen
        spanishAiportsType.collect().forEach(type -> System.out.println(type._2 + ": " +  type._1));

        // Step 7. Save results as text file
        spanishAiportsType.saveAsTextFile("output_spanish_airports");

        // Step 8. Close the SparkContext object to release the resources allocated
        sparkContext.close();

    }
}
