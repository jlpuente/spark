from pyspark.sql import SparkSession
from pyspark.sql.functions import length, lower, regexp_replace, explode, split, desc


def ten_most_repeated_words(data_frame) -> None:
    """
    Search for the 10 most repeated words and print them on screen sorted by frequency.
    Common words such as "RT", "http", etc, should be filtered.
    :param data_frame: DataFrame
    :return: None
    """
    # Step 1. Filter out unwanted words and characters and split tweets into words
    words_df = (data_frame
                .select(lower(regexp_replace(data_frame["Tweet"], "[\W_]+", " ")).alias("tweets"))
                .select(explode(split("tweets", "\\s+")).alias("word"))
                .filter(~(
                data_frame["Tweet"].like("RT%") | data_frame["Tweet"].like("%http://%") | data_frame["Tweet"].like(
            "%https://%") | data_frame["Tweet"].like("")))
                )

    # Step 2. Group by word and count the number of occurrences
    word_count_df = words_df.groupBy("word").count()

    # Step 3. Sort by count in descending order and get the top 10 words
    top_words = word_count_df.orderBy(desc("count")).limit(10)

    # Step 4. Extract word and count from each row and print them
    print('Task 1. The 10 most repeated words.')
    for pair in top_words.collect():
        print(f'{pair["word"]}: {pair["count"]}')


def user_most_active(data_frame) -> None:
    """
    Find the user who has written the most number of tweets and print the username and the number of tweets
    :param data_frame: DataFrame
    :return: None
    """

    # Step 1. Group by users and do the count
    user_tweet_count_df = data_frame.groupBy('User').count()

    # Step 2. Order the previous resulting dataframe by count column in an descending way and pick the first row
    most_tweets_user = user_tweet_count_df.orderBy('count', ascending=False).first()

    # Step 3. Print user and count of tweets
    print('\nTask 2. The most active user.')
    print("The most active user is {} with {} tweets".format(most_tweets_user[0], most_tweets_user[1]))


def the_shortest_tweet(data_frame) -> None:
    """
    Find the shortest tweet and print the user, length of the tweet and timestamp.
    :param data_frame: DataFrame
    :return: None
    """

    tweets_df = data_frame.withColumn("Length of tweet", length(data_frame["Tweet"])).cache()

    shortest_tweet = tweets_df.orderBy("Length of tweet", ascending=True).first()

    user_index = 1
    tweet_index = 2
    timestamp_index = 3
    length_index = len(tweets_df.columns) - 1

    print(f"\nTask 3. The shortest tweet\n"
          f"User: {shortest_tweet[user_index]}\n"
          f"Tweet: {shortest_tweet[tweet_index]}\n"
          f"Tweet length: {shortest_tweet[length_index]}\n"
          f"Timestamp: {shortest_tweet[timestamp_index]}")


if __name__ == "__main__":

    """
    This program reads 'tweets.tsv' file and performs the following 3 tasks using PySpark with RDDs:
    1. Search for the 10 most repeated words and print them on screen sorted by frequency
    Common words such as "RT", "http", etc, should be filtered
    2. Find the user who has written the most number of tweets and print the user name and the number of tweets
    3. Find the shortest tweet and print the user, length of the tweet and timestamp
    """

    # Step 1. Initialize a SparkSession object
    spark_session = SparkSession \
        .builder \
        .master("local[4]") \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    # Step 2. Read tweets.tsv file as data frame
    data_frame = spark_session \
        .read \
        .options(header='false', inferschema='true') \
        .option("delimiter", "\t") \
        .csv("../rdd/tweets.tsv") \
        .persist()

    data_frame = data_frame.withColumnRenamed('_c0', 'UUID')\
        .withColumnRenamed('_c1', 'User')\
        .withColumnRenamed('_c2', 'Tweet')\
        .withColumnRenamed('_c3', 'Timestamp')\
        .withColumnRenamed('_c4', 'Latitude')\
        .withColumnRenamed('_c5', 'Longitude')\
        .withColumnRenamed('_c6', 'City')

    # Step 3.1. Do the first task
    ten_most_repeated_words(data_frame)

    # Step 3.2. Do the second task
    user_most_active(data_frame)

    # Step 3.3. Do the third task
    the_shortest_tweet(data_frame)

    # Step 4. Close the SparkContext object
    spark_session.stop()
