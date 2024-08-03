from typing import List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, udf
from pyspark.sql.types import StringType, ArrayType
import emoji as emojilib

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Get the 10 most used emojis from tweets with their count using Spark.
    
    Args:
        file_path (str): Path to the JSON file containing tweets.

    Returns:
        List[Tuple[str, int]]: Top 10 emojis with their counts.
    """
    def extract_emojis(content: str) -> List[str]:
        """Extract emojis from a string."""
        return [char for char in content if char in emojilib.EMOJI_DATA]

    # Define UDF to extract emojis
    extract_emojis_udf = udf(extract_emojis, ArrayType(StringType()))

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("TopEmojis") \
        .getOrCreate()

    # Read JSON file into a DataFrame
    df = spark.read.json(file_path)

    # Extract and explode emojis
    emojis_df = df.withColumn("emojis", extract_emojis_udf(col("content")))
    exploded_df = emojis_df.withColumn("emoji", explode(col("emojis")))

    # Count and get top 10 emojis
    top_emojis = exploded_df.groupBy("emoji").count().orderBy(col("count").desc()).limit(10)

    # Collect results as list of tuples
    result = [(row['emoji'], row['count']) for row in top_emojis.collect()]

    spark.stop()
    
    return result
