from pyspark.sql import SparkSession
from pyspark.sql.functions import count


def main(airports_dataframe, countries_dataframe) -> None:

    # Step 1. Join both dataframes on ISO country code
    join_dataframe = airports_dataframe\
        .join(countries_dataframe, airports_dataframe.iso_country == countries_dataframe.code, 'inner')

    # Step 2. Filter in large airports by using where clause (which is an alias for filter, according to doc)
    large_join_dataframe = join_dataframe.where(join_dataframe["type"] == "large_airport")

    # Step 3.1. Group by 'country' column
    # Step 3.2. Count the number of rows for each partition
    # Step 3.3. Rename 'count(1)' resulting from aggregation function as 'count'
    # Step 3.4. Sort by 'count' column in descending direction
    # Step 3.5. Limit to 10 the dataframe
    # Step 3.6. Show resulting dataframe
    large_join_dataframe\
        .groupBy('country')\
        .agg(count('*'))\
        .withColumnRenamed('count(1)', 'count')\
        .orderBy('count', ascending=False)\
        .limit(10)\
        .show()


if __name__ == '__main__':

    """
    This is a PySpark program using DataFrame object to find the 10 countries with the most number of
    'large' airports. 'Large' is a category within 'airports.csv' and country names are found in 
    'countries.csv' file. The output should be a two-column table, whose header is the country name and count,
    displaying the result on screen.
    """
    # Step 1. Initialize a SparkSession object
    spark_session = SparkSession \
        .builder \
        .master("local[4]") \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    # Step 2. Read airports.csv file as data frame
    airports_dataframe = spark_session \
        .read \
        .options(header='true', inferschema='true') \
        .option("delimiter", ",") \
        .csv("airports.csv") \
        .persist()

    # Step 3. Read countries.csv file as data frame
    countries_dataframe = spark_session \
        .read \
        .options(header='true', inferschema='true') \
        .option("delimiter", ",") \
        .csv("countries.csv") \
        .persist()

    # Step 3.1. Uncomment to preview the input dataframes
    # airports_dataframe.show()
    # countries_dataframe.show()

    # Step 3.2. To avoid ambiguity, change 'name' columns
    airports_dataframe = airports_dataframe.withColumnRenamed('name', 'airport')
    countries_dataframe = countries_dataframe.withColumnRenamed('name', 'country')

    # Step 4. Do the main task
    main(airports_dataframe, countries_dataframe)

    # Final Step. Close SparkSession object to release the allocated resources
    spark_session.stop()
