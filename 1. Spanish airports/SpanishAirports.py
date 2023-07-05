from pyspark import SparkContext, SparkConf
import csv

'''
RDD-based PySpark program that reads a CSV file,
which must be in the root directory of the project,
and counts the number of Spanish airports by type of airport.
'''


def main() -> None:

    # Step 1. Create a SparkConf object
    spark_conf = SparkConf().setAppName("SpanishAirportsCounter").setMaster("local[4]")

    # Step 2. Create a SparkContext object
    spark_context = SparkContext(conf=spark_conf)

    # Step 3. Read the CSV file
    lines = spark_context.textFile("airports.csv")

    # Step 3.1. Store Spanish ISO country code and header indices
    spanish_iso_code = "ES"
    iso_country_index = 8
    airport_type_index = 2

    # Step 4. Parse the CSV data and filter for Spanish airports by using csv Python module
    spanish_airports_rdd = lines \
        .map(lambda line: next(csv.reader([line]))) \
        .filter(lambda fields: fields[iso_country_index] == spanish_iso_code)

    # Step 5. Count the number of Spanish airports by type of airport
    airport_type_counts = spanish_airports_rdd \
        .map(lambda fields: (fields[airport_type_index], 1)) \
        .reduceByKey(lambda x, y: x + y)

    # Step 6. Print the output
    for airport_type, count in airport_type_counts.collect():
        print(f'{airport_type}: {count}')

    # Step 7. Save output at text file
    airport_type_counts.saveAsTextFile("output_spanish_airports_pyspark")

    # Stop the SparkContext
    spark_context.stop()


if __name__ == "__main__":

    main()
