from pyspark import SparkConf, SparkContext
import re


def filter_starts_with(string, filter_list) -> str:
    """Filters out substrings that start with any string in the filter_list."""
    pattern = '|'.join(filter_list)
    filtered_string = re.sub(r'\b(?:{})\S*\s?'.format(pattern), '', string)
    return filtered_string


def is_empty_char(word) -> bool:
    """Returns whether the input word is an empty char or not"""
    return word == ''


def ten_most_repeated_words(rdd_lines) -> None:
    """
    Search for the 10 most repeated words and print them on screen sorted by frequency.
    Common words such as "RT", "http", etc, should be filtered.
    :param rdd_lines: RDD of text lines
    :return: None
    """

    # Step 1. For each line pick the tweet
    tweet_index = 2
    tweets = rdd_lines.map(lambda line: line[tweet_index])

    # Step 2. Set the list of words to filter out the dataset. They appear in nearly all tweets
    filter_list = ['RT', 'http://', 'https://']

    # Step 3. Filter out the words
    tweets_filtered = tweets.map(lambda tweet: filter_starts_with(tweet, filter_list))

    # Step 4. Create a new dataset composed of words
    words = tweets_filtered.flatMap(lambda tweet: tweet.split(' '))

    # Step 5. Filter out the empty char (''), the dot ('.'), the comma (',') and the colon (':')
    filter_empty_chars = words.filter(lambda word: not is_empty_char(word))
    words_filtered = filter_empty_chars.map(lambda word: word.strip('.'))
    words_filtered_2 = words_filtered.map(lambda word: word.strip(','))
    words_filtered_3 = words_filtered_2.map(lambda word: word.strip(':'))

    # Step 6. For each word map into a pair of (word, 1)
    pairs = words_filtered_3.map(lambda word: (word, 1))

    # Step 7. Reduce the dataset to unique keys and sum values
    word_counter = pairs.reduceByKey(lambda value_1, value_2: value_1 + value_2)

    # Step 8. Sort by the element 1 of the pair (which is the value) in a descending way
    word_counter_sorted = word_counter.sortBy(lambda pair: pair[1], ascending=False)

    # Step 9. Take the 10 most repeated words in the dataset
    ten_most_repeated_words = word_counter_sorted.take(10)

    # Step 10. Print them on screen
    print('Task 1. The 10 most repeated words.\n')
    for index, tuple in enumerate(ten_most_repeated_words):
        print(f'{index + 1}.\t{tuple}')


def most_active_active(rdd_lines) -> None:
    """
    Find the user who has written the most number of tweets and print the username and the number of tweets
    :param rdd_lines: RDD of text lines
    :return: None
    """

    # Step 1. For each line pick the user
    user_index = 1
    users = rdd_lines.map(lambda line: line[user_index])

    # Step 2. For each user map into a pair (user, 1)
    pairs = users.map(lambda user: (user, 1))

    # Step 3. Reduce the dataset to unique user names and sum values
    user_count = pairs.reduceByKey(lambda v1, v2: v1 + v2)

    # Step 4. Sort by the element 1 of the pair (which is the value) in a descending way
    user_count_sorted = user_count.sortBy(lambda pair: pair[1], ascending=False)

    # Step 5. Pick the first user. Since collect() method returns a list, we must access to the first element
    user = user_count_sorted.take(1)[0]

    # Step 6. Print the user on screen and the number of tweets
    print(f'\nTask 2. The most active user.\n'
          f'The most active user is {user[0]}, who has written {user[1]} tweets.')


def the_shortest_tweet(rdd_lines) -> None:
    """
    Find the shortest tweet and print the user, length of the tweet and timestamp.
    :param rdd_lines: RDD of text lines
    :return: None
    """

    # Step 1. Map each tweet into a tuple containing the username, tweet, tweet length and timestamp
    user_index = 1
    tweet_index = 2
    timestamp_index = 3

    tweets_tuple_rdd = rdd_lines.map(lambda tweet: (tweet[user_index], tweet[tweet_index], len(tweet[tweet_index]), tweet[timestamp_index]))

    # Step 2. Sort each tweet by length of tweet
    shortest_tweet = tweets_tuple_rdd.sortBy(lambda tweet: tweet[2]).first()

    # Step 3. Print the result on screen
    print(f'\nTask 3. The shortest tweet.\n'
          f'User: {shortest_tweet[0]}\n'
          f'Tweet: {shortest_tweet[1]}\n'
          f'Tweet length: {shortest_tweet[2]}\n'
          f'Timestamp: {shortest_tweet[3]}')

if __name__ == "__main__":

    """
    This program reads 'tweets.tsv' file and performs the following 3 tasks using PySpark with DataFrames:
    1. Search for the 10 most repeated words and print them on screen sorted by frequency
    Common words such as "RT", "http", etc, should be filtered
    2. Find the user who has written the most number of tweets and print the user name and the number of tweets
    3. Find the shortest tweet and print the user, length of the tweet and timestamp
    """

    # Step 1. Initialize a SparkConf object
    spark_conf = SparkConf().setAppName('CSVCleaner').setMaster('local[4]')

    # Step 2. Create a SparkContext object
    spark_context = SparkContext.getOrCreate(spark_conf)

    # Step 3. Read tweets.tsv file into lines
    lines = spark_context.textFile("tweets.tsv").cache()

    # Step 4. For each line map into a RDD of strings
    rdd_lines = lines.map(lambda line: line.split('\t'))

    # Step 5.1. Do the first task
    ten_most_repeated_words(rdd_lines)

    # Step 5.2. Do the second task
    most_active_active(rdd_lines)

    # Step 5.3. Do the third task
    the_shortest_tweet(rdd_lines)

    # Step 6. Close the SparkContext object
    spark_context.stop()
