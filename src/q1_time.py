from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, max as spark_max, count, desc
from pyspark.sql.types import DateType, StringType
from typing import List, Tuple

def q1_time(file_path: str) -> List[Tuple[str, str, int]]:
    """
    Get the top 10 dates with the most tweets and the username with the most posts for each day from a JSON file
    using Apache Spark.

    Args:
        file_path (str): The path to the JSON file containing the tweets.

    Returns:
        List[Tuple[str, str, int]]: A list of tuples where each tuple contains a date (y-m-d),
        the username with the most posts on that date, and the number of tweets on that date.
    """
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("TweetAnalysis") \
        .getOrCreate()
    
    # Read JSON file into DataFrame and process it
    df = spark.read.json(file_path) \
        .withColumn("date", col("date").cast(DateType())) \
        .withColumn("username", col("user.username")) \
        .select("date", "username")

    # Caching DataFrame
    df.cache()

    # Aggregate total tweets and tweets per user per date
    date_totals = df.groupBy("date") \
        .agg(count("*").alias("total_tweets"))

    date_user_counts = df.groupBy("date", "username") \
        .agg(count("*").alias("tweet_user_count"))

    # Find the maximum tweet count per user per date
    date_user_max_counts = date_user_counts.groupBy("date") \
        .agg(spark_max("tweet_user_count").alias("max_tweets"))

    # Join to get the user with the max tweet count per date
    result = date_user_counts.alias("user_counts") \
        .join(date_user_max_counts.alias("max_counts"), ["date"]) \
        .filter(col("user_counts.tweet_user_count") == col("max_counts.max_tweets"))

    # Combine total tweet count with result, order by total tweets, and limit to top 10
    final_result = date_totals.join(result, on="date") \
        .orderBy(col("total_tweets").desc()) \
        .limit(10) \
        .select(date_format("date", "yyyy-MM-dd").alias("date"),
                col("username"))

    # Convert the DataFrame to a list of tuples
    results = [(row["date"], row["username"]) for row in final_result.collect()]
            
    return results
