from pyspark import SparkContext, SparkConf


def main() -> None:

    # Step 1. Create a SparkConf object
    conf = SparkConf().setAppName("FilmLocations").setMaster("local[4]")

    # Step 2. Initialize a SparkContext object and turn off warning messages
    spark_context = SparkContext.getOrCreate(conf)

    # Step 3. Load CSV file
    rdd = spark_context.textFile("Film_Locations_in_San_Francisco.csv").cache()

    # Step 3.1. Set title index
    title_index = 0

    # Step 4. Map RDD pairs of (film, number of locations)
    rdd_pairs = rdd.map(lambda line: (line.split(',')[title_index], 1)).cache()

    # Step 5. Reduce by key to count the number of locations by film
    rdd_counts = rdd_pairs.reduceByKey(lambda a, b: a + b).cache()

    # Step 6. Map RDD pairs of (film, number of locations) into (number of locations, film)
    rdd_pairs_count = rdd_counts.map(lambda pair: (pair[1], pair[0])).cache()

    # Step 7. Print the RDD pairs
    for pair in rdd_pairs_count.collect():
        print(pair)

    # Step 8. Print total number of films and average locations per film
    total_films = rdd_pairs_count.count()
    total_locations = rdd_pairs_count.map(lambda pair: pair[0]).reduce(lambda a, b: a + b)
    avg_locations = total_locations / float(total_films)
    print("Total number of films:", total_films)
    print("The average of film locations per film:", avg_locations)

    rdd_pairs_count.saveAsTextFile("output_san_francisco_pyspark")

    # Stop SparkContext
    spark_context.stop()


if __name__ == "__main__":

    main()
