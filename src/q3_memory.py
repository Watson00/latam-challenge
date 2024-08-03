from typing import List, Tuple
from collections import Counter
import json
import re

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Get the top 10 most mentioned users with their count from a tweets JSON file.
    Optimized for memory performance.

    Args:
        file_path (str): The path to the JSON file containing the tweets.

    Returns:
        List[Tuple[str, int]]: A list of tuples where each tuple contains a user
        and its count.
    """
    # Counter to track mentions
    user_counts = Counter()

    # Process each line in the JSON file
    with open(file_path, 'r') as file:
        for line in file:
            try:
                tweet = json.loads(line)
                content = tweet.get('content', '')
                if content:
                    mentions = re.findall(r'@\w+', content)
                    # Update the counter with mentions
                    user_counts.update(mentions)
            except json.JSONDecodeError:
                print("Skipping invalid JSON line.")

    # Get the top 10 most mentioned users
    top_users = user_counts.most_common(10)

    return top_users