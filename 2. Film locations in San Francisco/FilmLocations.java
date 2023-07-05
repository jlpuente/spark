package org.jlpuente.RDD;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * A RDD-based Spark program in Java that reads "airports.csv",
 * which is located in the root directory of the project,
 * and finds all those films with 20 or more locations.
 * Finally the program calculates the number of films and the average of locations per film.
 */

public class FilmLocations {

    public static void main(String[] args) {

        // Step 0 (Optional). Turn off Spark log messages
        Logger.getRootLogger().setLevel(Level.OFF);

        // Step 1. Create a SparkConf object
        SparkConf conf = new SparkConf()
                .setAppName("FilmLocations")
                .setMaster("local[4]");

        // Step 2. Create a SparkContext object
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Step 3. Load the data into an RDD
        JavaRDD<String> lines = sparkContext.textFile("Film_Locations_in_San_Francisco.csv");

        // Step 4. Read the header
        String header = lines.first();

        // Step 4.1. Store indices: film index is 0 and location index is 2 in the header
        int filmIndex = 0;
        int locationIndex = 2;

        // Step 5. Remove header from RDD
        JavaRDD<String> data = lines
                .filter(line -> !line.contains(header))
                .cache();

        // Step 6. Select title and location columns by mapping them into a pair (title, location)
        JavaPairRDD<String, String> pairs = data.mapToPair(film -> new Tuple2<>(film.split(",")[filmIndex],
                film.split(",")[locationIndex])).cache();

        // Step 7. Map each pair into a new pair of (title, 1)
        JavaPairRDD<String, Integer> pairs2 = pairs.mapToPair(pair -> new Tuple2<>(pair._1(), 1)).cache();

        // Step 8. Sum the number of locations by title
        JavaPairRDD<String, Integer> titlesByLocation = pairs2.reduceByKey((v1, v2) -> v1 + v2).cache();

        // Step 9. Switch the components for each tuple
        JavaPairRDD<Integer, String> locationCounts = titlesByLocation.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1())).cache();

        // Step 10. Sort pairs by key
        JavaPairRDD<Integer, String> output = locationCounts.sortByKey().cache();

        // Step 11. Count the number of films
        long numberOfFilms = output.count();

        // Step 12. Sum locations for each film
        long totalLocations = output.map(tuple -> tuple._1).reduce(((integer, integer2) -> integer + integer2));

        // Step 13. Calculate the average of locations per each film
        double avgLocationsByFilm =  (double) numberOfFilms / (double) totalLocations * 100;

        // Step 11. Print the results
        output.collect().forEach(tuple -> System.out.println("(" + tuple._1 + ", " +  tuple._2 + ")"));
        System.out.println("\nTotal number of films: " + numberOfFilms);
        System.out.println("The average of film locations per film: " + avgLocationsByFilm);

        // Step 12. Save output as text file
        output.saveAsTextFile("output_san_francisco");

        // Step 13. Close SparkContext object to release the resources allocated in memory
        sparkContext.close();

    }

}
