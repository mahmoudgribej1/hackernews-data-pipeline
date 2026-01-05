import pandas as pd
import json
import os
from datetime import datetime
from urllib.parse import urlparse

def get_latest_data_path():
    # Finds the folder with today's date
    today = datetime.now().strftime("%Y-%m-%d")
    return f"data/raw/{today}/stories.json"

def transform_data():
    input_path = get_latest_data_path()
    
    if not os.path.exists(input_path):
        print("Raw data file not found!")
        return

    # 1. Load Data
    df = pd.read_json(input_path)
    print(f"Loaded {len(df)} stories.")

    # 2. Basic Cleaning
    # Drop stories that were deleted or have no title
    df = df.dropna(subset=['title'])
    df = df[df['type'] == 'story']

    # 3. Time Transformations
    # Convert Unix timestamp (1704153600) to datetime object
    df['posted_at'] = pd.to_datetime(df['time'], unit='s')
    df['hour_posted'] = df['posted_at'].dt.hour
    df['is_weekend'] = df['posted_at'].dt.dayofweek >= 5

    # 4. Feature Engineering: Extract Domain
    # e.g., "https://github.com/openai" -> "github.com"
    df['domain'] = df['url'].dropna().apply(lambda x: urlparse(x).netloc)

    # 5. Categorization
    def categorize(title):
        if "Show HN" in title: return "Show HN"
        if "Ask HN" in title: return "Ask HN"
        return "Article"

    df['story_type'] = df['title'].apply(categorize)

    # 6. Save Processed Data (as CSV for easy viewing)
    output_dir = input_path.replace("raw", "processed").replace("stories.json", "")
    os.makedirs(output_dir, exist_ok=True)
    
    df.to_csv(f"{output_dir}/stories_cleaned.csv", index=False)
    print(f"Transformation complete. Saved to {output_dir}")

if __name__ == "__main__":
    transform_data()