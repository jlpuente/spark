from pyspark.sql import SparkSession, functions
import re
import time

# Author: José Luis Puente Bodoque
# Date: April 1, 2023
# Version: 1.0

def clean_data(dataframe):
    """
    Does a particular cleaning on our work dataframe. Removes padding characters, splits a column into two, drop
    columns full of null values and rearranges the order of the column.
    :param dataframe: pyspark.sql.DataFrame
    :return: dataframe: pyspark.sql.DataFrame
    """

    # Step 1. Clean first column: remove double quotes from the entire column, including this
    column_name = dataframe.columns[0]
    expr_quote = "\""
    dataframe = dataframe\
        .withColumn(re.sub(expr_quote, "", column_name), functions.regexp_replace(column_name, expr_quote, ""))

    # Step 2. Drop columns from 1960 to 1989 and 2021, which have null values
    years = []
    for year in range(1960, 1990, 1):
        years.append(str(year))
    years.append(str(2021))

    dataframe = dataframe.drop(*years)  # drop columns from 1960 to 1989
    dataframe = dataframe.drop(column_name)  # drop the first column
    dataframe = dataframe.drop(dataframe[-2])  # drop the penultimate column (the last one in the original dataframe)

    # Step 3. Split the first column into two new ones and drop the old one
    last_column_name = str(dataframe.columns[-1])
    expr_comma = ","
    dataframe = dataframe.withColumn("Country Name", functions.split(last_column_name, expr_comma)[0]) \
        .withColumn("Country Code", functions.split(last_column_name, expr_comma)[1]) \
        .drop(last_column_name)

    # Step 4. Rearrange the order of the columns. Move the last two columns to the first positions
    penultimate_column_name = dataframe.columns[-2]
    last_column_name = dataframe.columns[-1]
    dataframe = dataframe\
        .select(functions.col(dataframe.columns[-2])
                .alias(penultimate_column_name),
                functions.col(dataframe.columns[-1])
                .alias(last_column_name),
                *dataframe.columns[:-2])

    # Now the dataframe is clean :) and suitable to work on it
    return dataframe


def main() -> None:

    # Step 1. Initialize a SparkSession object
    spark_session = SparkSession \
        .builder \
        .master("local[4]") \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    # Step 1.5. (Optional) My machine takes as maximum 15 s to display all messages related to Spark.
    # I cannot filter them out, so I decided to wait for them
    to_wait_for_spark_messages = 15
    time.sleep(to_wait_for_spark_messages)

    # Note before step 2: after exploring the data file I noticed the values are enclosed by double quotes twice
    # ("\"\"value\"\"") so that the "value" inferred is something like "\"value\"" instead of the expected "value".
    # So, before reading the file, I have replaced every "\"\"" with "\"" on the PyCharm text editor.

    # Step 2.a. Read the CSV input data file as DataFrame object and store it in memory for working on it onwards
    # The file has header, and we want the SparkSession object to infer the data types, too
    mess_dataframe = spark_session \
        .read \
        .options(header='true', inferschema='false') \
        .option("delimiter", '","') \
        .csv("API_EG.ELC.ACCS.ZS_DS2_en_csv_v2_5358776.csv") \
        .persist()

    # Step 2.b. Read the CSV input metadata file as DataFrame object
    metaframe = spark_session \
        .read \
        .options(header='true', inferschema='false') \
        .option("delimiter", ',') \
        .csv("Metadata_Country_API_EG.ELC.ACCS.ZS_DS2_en_csv_v2_5358776.csv") \
        .persist()

    # Step 3.1. Preview of metadata. The last column is null, but it does not disturb
    # metaframe.show()

    # Step 3.2. Preview of data. The dataframe is quite a mess
    # dataframe.show()

    # Step 4. Exploratory data analysis by displaying basic statistics, e.g. to discover values out of range
    # dataframe.describe().show()

    # Step 5. Clean the dataframe
    dataframe = clean_data(mess_dataframe)
    # dataframe.show()

    # Step 6. Now I already have a quite dataframe. Join data and metadata dataframes on 'Country Code' column
    dataframe = dataframe.join(metaframe, on='Country Code')

    # Step 7. Input the start and end years from keyboard
    start_year = input("Enter the start year (between 1990 and 2020): ")
    end_year = input("Enter the end year (between 1990 and 2020): ")

    start_year = int(start_year)
    end_year = int(end_year)

    # Step 8. Select the columns for the specified range of years
    year_cols = [str(year) for year in range(start_year, end_year + 1)]
    selected_cols = ["Region"] + year_cols
    dataframe = dataframe.select(selected_cols)

    # Step 9. Filter the data to include only rows with at least 99 % of electricity access for the range
    dataframe_filtered = dataframe.filter((functions.col(year_cols[0]) >= 99) & (functions.col(year_cols[-1]) >= 99))
    for year in year_cols[1:-1]:
        dataframe_filtered = dataframe_filtered.filter(functions.col(year) >= 99)

    # Step 10. Compute the average percentage for each region
    dataframe_avg = dataframe_filtered\
        .groupBy("Region").agg(functions.avg(functions.col(year_cols[-1])).alias("Percentage (%)"))

    # Step 11. Sort the data in descending order by percentage and drop rows with null values, which corresponds to
    # large aggregations of countries and not actually regions
    dataframe_result = dataframe_avg.sort(functions.col("Percentage (%)").desc()).dropna()

    # Step 12. Show the output
    dataframe_result.show()

    # Final step. Stop the SparkSession object to release the allocated resources
    spark_session.stop()


if __name__ == '__main__':

    """
    This PySpark program uses DataFrame objects to do the main task.
    Entered a range of years as input, the main task prints on screen those regions whose average of electricity
    access is equal or greater than 99 %. The output must be ordered in descending direction by the percentage value.
    There is a data file, which contains mainly the countries and the years, and a metadata file, which contains
    the regions which each country belongs to. This programs load both files as DataFrames objects (dataframe and
    metaframe), clean the dataframe and finally join both dataframes to produce the expected output. 
    """

    main()
