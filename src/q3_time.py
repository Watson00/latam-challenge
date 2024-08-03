from typing import List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, udf
from pyspark.sql.types import ArrayType, StringType
import re
 
def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Get the top 10 most mentioned users with their count from a tweets JSON file using Spark.
    
    Args:
        file_path (str): The path to the JSON file containing the tweets.

    Returns:
        List[Tuple[str, int]]: A list of tuples where each tuple contains an user
        and its count.
    """
    # Define UDF to extract mentions from content
    def extract_mentions(content: str) -> List[str]:
        """Extract mentions (usernames) from tweet content."""
        return re.findall(r'@\w+', content)
    
    spark = SparkSession.builder \
        .appName("TopMentions") \
        .getOrCreate()

    extract_mentions_udf = udf(extract_mentions, ArrayType(StringType()))
    df = spark.read.json(file_path)

    # Extract mentions from 'content'
    df_mentions = df.withColumn("mentions", extract_mentions_udf(col("content")))

    # Explode the mentions into individual rows
    exploded_df = df_mentions.withColumn("mention", explode(col("mentions")))

    # Count occurrences of each mention
    mention_counts = exploded_df.groupBy("mention").count()

    # Get the top 10 most mentioned users
    top_mentions = mention_counts.orderBy(col("count").desc()).limit(10)

    # Collect results and convert to list of tuples
    result = [(row['mention'], row['count']) for row in top_mentions.collect()]

    spark.stop()
    
    return result
