from typing import List, Tuple
from datetime import datetime
from collections import defaultdict
import emoji as emojilib
import json

def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Get the 10 most used emojis from tweets with their count from a JSON file.
    
    Args:
        file_path (str): The path to the JSON file containing the tweets.

    Returns:
        List[Tuple[str, int]]: A list of tuples with the top 10 emojis and their counts.
    """
    # Dictionary to count occurrences of each emoji
    emojis_counts = defaultdict(int)

    with open(file_path, 'r') as file:
        for line in file:
            try:
                tweet = json.loads(line)
                content = tweet.get('content', '')
                if content: # Check if content is valid
                    # Extract and count emojis
                    emojis = [c for c in content if c in emojilib.EMOJI_DATA]
                    for e in emojis:
                        emojis_counts[e] += 1
            except json.JSONDecodeError:
                print("Skipping invalid JSON line.")

    # Get the top 10 emojis by count
    top_emojis = sorted(emojis_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    
    return top_emojis