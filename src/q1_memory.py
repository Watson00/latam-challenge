from typing import List, Tuple
from datetime import datetime
from collections import defaultdict
import json

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Get the top 10 dates with the most tweets and the username with the most posts for each day from a JSON file.
    Optimized for memory perfomance

    Args:
        file_path (str): The path to the JSON file containing the tweets.

    Returns:
        List[Tuple[datetime.date, str]]: A list of tuples where each tuple contains a date (y-m-d)
        and the username with the most posts on that date.
    """
    # Dictionary to hold the count of tweets per user per date
    date_user_counts = defaultdict(lambda: defaultdict(int))

    with open(file_path, 'r') as file:
        for line in file:
            try:
                tweet = json.loads(line)

                # Extract the tweet date and username
                date = datetime.fromisoformat(tweet['date']).date()
                username = tweet['user']['username'] if tweet['user'] and 'username' in tweet['user'] else None
                
                # Update the tweet count for the date and user
                if username: # Only increment count if username is valid
                    date_user_counts[date][username] += 1
            except json.JSONDecodeError:
                print("Skipping invalid JSON line.")

    # Sort dates by total tweet counts and get the top 10
    top_dates = sorted(date_user_counts.items(), key=lambda x: sum(x[1].values()), reverse=True)[:10]
    
    # For each of the top dates, find the user with the maximum number of posts
    result = [
        (date.strftime('%Y-%m-%d'), max(users.items(), key=lambda x: x[1])[0]) 
        for date, users in top_dates
    ]
    
    return result