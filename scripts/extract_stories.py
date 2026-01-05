import requests
import json
import os
import boto3
from datetime import datetime
import time

# --- Configurations ---
BASE_URL = "https://hacker-news.firebaseio.com/v0"
# We want a mix of stories to make the analysis interesting
LIMIT_TOP = 200
LIMIT_NEW = 200
LIMIT_BEST = 100
TOTAL_TARGET = LIMIT_TOP + LIMIT_NEW + LIMIT_BEST

def fetch_story_ids(endpoint, limit):
    """Fetches a list of story IDs from a specific HN endpoint."""
    url = f"{BASE_URL}/{endpoint}.json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        ids = response.json()
        print(f"Fetched {len(ids)} IDs from {endpoint}")
        return ids[:limit]
    except Exception as e:
        print(f"Error fetching {endpoint}: {e}")
        return []

def fetch_story_details(story_id):
    """Fetches details for a single story ID."""
    url = f"{BASE_URL}/item/{story_id}.json"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Failed to fetch story {story_id}: {e}")
    return None

def upload_to_s3(local_file_path, bucket_name, s3_key):
    """Uploads a local file to AWS S3."""
    print(f"☁️  Uploading to S3 bucket: {bucket_name}...")
    
    # Initialize S3 Client using credentials from .env
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'eu-central-1')
    )
    
    try:
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"✅ Success! Uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"❌ Failed to upload to S3: {e}")

def save_raw_data(stories):
    """Saves the list of stories to a JSON file partitioned by date."""
    today = datetime.now().strftime("%Y-%m-%d")
    output_dir = f"data/raw/{today}"
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{output_dir}/stories.json"
    
    with open(filename, "w") as f:
        json.dump(stories, f, indent=4)
    
    print(f"Successfully saved {len(stories)} stories to {filename}")

    # 2. S3 UPLOAD (The new Data Lake layer)
    bucket_name = os.getenv("S3_BUCKET_NAME")
    
    if bucket_name:
        # We start the S3 path with 'raw/' to organize our bucket
        s3_key = f"raw/{today}/stories.json"
        upload_to_s3(filename, bucket_name, s3_key)
    else:
        print("⚠ S3_BUCKET_NAME not found in environment variables. Skipping upload.")

def main():
    print("--- Starting HN Data Extraction ---")
    
    # 1. Get Story IDs
    top_ids = fetch_story_ids("topstories", LIMIT_TOP)
    new_ids = fetch_story_ids("newstories", LIMIT_NEW)
    best_ids = fetch_story_ids("beststories", LIMIT_BEST)
    
    # Use a set to handle duplicates (e.g. a story can be Top AND Best)
    all_ids = list(set(top_ids + new_ids + best_ids))
    print(f"Unique stories to fetch: {len(all_ids)}")
    
    # 2. Fetch Details for each story
    stories = []
    for i, story_id in enumerate(all_ids):
        story = fetch_story_details(story_id)
        if story and story.get("type") == "story": # Filter ensures we only get stories, not comments
            stories.append(story)
        
        # Simple progress indicator
        if (i + 1) % 50 == 0:
            print(f"Processed {i + 1}/{len(all_ids)}...")
            
        # Optional: Sleep briefly to be nice to the API (HN is lenient, but good practice)
        time.sleep(0.1) 

    # 3. Save Data
    if stories:
        save_raw_data(stories)
    else:
        print("No stories found.")

if __name__ == "__main__":
    main()