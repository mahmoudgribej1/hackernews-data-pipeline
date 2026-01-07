import requests
import json
import os
import boto3
from datetime import datetime
import time
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Configurations ---
BASE_URL = "https://hacker-news.firebaseio.com/v0"
LIMIT_TOP = 200
LIMIT_NEW = 200
LIMIT_BEST = 100
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
REQUEST_TIMEOUT = 10  # seconds

def fetch_story_ids(endpoint, limit):
    """Fetches story IDs with retry logic and error handling"""
    url = f"{BASE_URL}/{endpoint}.json"
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Fetching {endpoint} (attempt {attempt + 1}/{MAX_RETRIES})")
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            ids = response.json()
            logger.info(f"✓ Fetched {len(ids)} IDs from {endpoint}")
            return ids[:limit]
            
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt + 1} for {endpoint}")
        except requests.exceptions.ConnectionError:
            logger.warning(f"Connection error on attempt {attempt + 1} for {endpoint}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code} for {endpoint}")
            if e.response.status_code == 429:  # Rate limit
                logger.warning("Rate limited! Waiting longer...")
                time.sleep(RETRY_DELAY * 5)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        
        # Exponential backoff between retries
        if attempt < MAX_RETRIES - 1:
            wait_time = RETRY_DELAY * (2 ** attempt)
            logger.info(f"Waiting {wait_time}s before retry...")
            time.sleep(wait_time)
    
    logger.error(f"Failed to fetch {endpoint} after {MAX_RETRIES} attempts")
    return []

def fetch_story_details(story_id):
    """Fetches details for a single story with retry logic"""
    url = f"{BASE_URL}/item/{story_id}.json"
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                time.sleep(1)
        except:
            if attempt < MAX_RETRIES - 1:
                time.sleep(0.5)
            continue
    return None

def check_if_already_extracted(output_dir):
    """
    Check if data for today already exists (idempotency)
    Returns: (exists: bool, count: int)
    """
    filename = f"{output_dir}/stories.json"
    
    if os.path.exists(filename):
        logger.info(f"Found existing data at {filename}")
        try:
            with open(filename, 'r') as f:
                existing_data = json.load(f)
            story_count = len(existing_data)
            logger.info(f"Existing file contains {story_count} stories")
            return True, story_count
        except json.JSONDecodeError:
            logger.warning("Existing file is corrupted, will re-extract")
            return False, 0
        except Exception as e:
            logger.warning(f"Error reading existing file: {e}")
            return False, 0
    
    return False, 0

def upload_to_s3(local_file_path, bucket_name, s3_key):
    """Uploads a local file to AWS S3 with error handling"""
    logger.info(f"☁️  Uploading to S3: s3://{bucket_name}/{s3_key}")
    
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION', 'eu-central-1')
        )
        
        # Upload with metadata
        s3.upload_file(
            local_file_path, 
            bucket_name, 
            s3_key,
            ExtraArgs={
                'Metadata': {
                    'upload-date': datetime.now().isoformat(),
                    'pipeline-stage': 'extraction'
                }
            }
        )
        logger.info(f"✓ Successfully uploaded to S3")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            logger.error(f"S3 bucket '{bucket_name}' does not exist!")
        elif error_code == 'AccessDenied':
            logger.error("Access denied! Check your AWS credentials")
        else:
            logger.error(f"S3 upload failed: {e}")
        return False
        
    except Exception as e:
        logger.error(f"Unexpected error during S3 upload: {e}")
        return False

def save_raw_data(stories):
    """Saves stories locally and uploads to S3"""
    today = datetime.now().strftime("%Y-%m-%d")
    output_dir = f"/opt/airflow/data/raw/{today}"
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{output_dir}/stories.json"
    
    # Save locally
    try:
        with open(filename, "w") as f:
            json.dump(stories, f, indent=2)  # indent=2 is more compact than 4
        logger.info(f"✓ Saved {len(stories)} stories to {filename}")
    except Exception as e:
        logger.error(f"Failed to save local file: {e}")
        raise
    
    # Upload to S3
    bucket_name = os.getenv("S3_BUCKET_NAME")
    if bucket_name:
        s3_key = f"raw/{today}/stories.json"
        upload_success = upload_to_s3(filename, bucket_name, s3_key)
        if not upload_success:
            logger.warning("⚠ S3 upload failed, but local file saved successfully")
    else:
        logger.warning("⚠ S3_BUCKET_NAME not set, skipping S3 upload")

def run_quality_checks(stories):
    """Basic quality checks on extracted data"""
    logger.info("Running quality checks...")
    
    issues = []
    
    # Check 1: Minimum story count
    if len(stories) < 400:
        issues.append(f"Low story count: {len(stories)} < 400")
    else:
        logger.info(f"✓ Story count OK: {len(stories)}")
    
    # Check 2: No duplicate IDs
    ids = [s.get('id') for s in stories]
    duplicates = len(ids) - len(set(ids))
    if duplicates > 0:
        issues.append(f"Found {duplicates} duplicate IDs")
    else:
        logger.info(f"✓ No duplicate IDs")
    
    # Check 3: Required fields
    required_fields = ['id', 'title', 'by', 'score', 'time']
    incomplete = 0
    for story in stories:
        if not all(field in story and story[field] is not None for field in required_fields):
            incomplete += 1
    
    if incomplete > len(stories) * 0.1:  # More than 10%
        issues.append(f"Too many incomplete stories: {incomplete}/{len(stories)}")
    else:
        logger.info(f"✓ Data completeness OK ({incomplete} incomplete stories)")
    
    # Check 4: Reasonable scores
    scores = [s.get('score', 0) for s in stories if s.get('score')]
    if scores:
        max_score = max(scores)
        if max_score > 10000:
            logger.warning(f"⚠ Unusually high score: {max_score}")
        else:
            logger.info(f"✓ Score values reasonable (max: {max_score})")
    
    # Check 5: Data freshness
    timestamps = [s.get('time') for s in stories if s.get('time')]
    if timestamps:
        newest = max(timestamps)
        age_hours = (datetime.now().timestamp() - newest) / 3600
        if age_hours > 48:
            logger.warning(f"⚠ Newest story is {age_hours:.1f} hours old")
        else:
            logger.info(f"✓ Data is fresh (newest: {age_hours:.1f}h old)")
    
    # Report issues
    if issues:
        logger.warning(f"⚠ Quality issues found:")
        for issue in issues:
            logger.warning(f"  - {issue}")
    else:
        logger.info("✓ All quality checks passed!")
    
    return len(issues) == 0

def main():
    logger.info("=" * 70)
    logger.info("STARTING HACKER NEWS DATA EXTRACTION")
    logger.info("=" * 70)
    
    start_time = time.time()
    today = datetime.now().strftime("%Y-%m-%d")
    output_dir = f"/opt/airflow/data/raw/{today}"
    
    # Idempotency check
    already_exists, existing_count = check_if_already_extracted(output_dir)
    if already_exists and existing_count >= 400:
        logger.info("✓ Sufficient data already exists for today")
        logger.info("Skipping extraction to avoid duplicates")
        logger.info("=" * 70)
        return
    
    # Fetch story IDs
    logger.info("Step 1/3: Fetching story IDs...")
    top_ids = fetch_story_ids("topstories", LIMIT_TOP)
    new_ids = fetch_story_ids("newstories", LIMIT_NEW)
    best_ids = fetch_story_ids("beststories", LIMIT_BEST)
    
    # Validate we got some data
    if not (top_ids or new_ids or best_ids):
        raise Exception("❌ Failed to fetch any story IDs from HN API!")
    
    all_ids = list(set(top_ids + new_ids + best_ids))
    logger.info(f"✓ Total unique stories to fetch: {len(all_ids)}")
    
    # Fetch story details
    logger.info("Step 2/3: Fetching story details...")
    stories = []
    failed_fetches = 0
    
    for i, story_id in enumerate(all_ids):
        story = fetch_story_details(story_id)
        
        if story and story.get("type") == "story":
            stories.append(story)
        else:
            failed_fetches += 1
        
        # Progress indicator
        if (i + 1) % 100 == 0:
            logger.info(f"Progress: {i + 1}/{len(all_ids)} processed ({len(stories)} valid)")
        
        # Rate limiting
        time.sleep(0.1)
    
    logger.info(f"✓ Successfully fetched {len(stories)} stories")
    if failed_fetches > 0:
        logger.warning(f"⚠ Failed to fetch {failed_fetches} stories")
    
    # Validate we got enough data
    if len(stories) < 300:
        raise Exception(f"❌ Insufficient stories fetched: {len(stories)} < 300")
    
    # Save data
    logger.info("Step 3/3: Saving data...")
    save_raw_data(stories)
    
    # Quality checks
    run_quality_checks(stories)
    
    # Summary
    duration = time.time() - start_time
    logger.info("=" * 70)
    logger.info(f"✓ EXTRACTION COMPLETED SUCCESSFULLY")
    logger.info(f"  Stories fetched: {len(stories)}")
    logger.info(f"  Failed fetches: {failed_fetches}")
    logger.info(f"  Duration: {duration:.1f}s")
    logger.info("=" * 70)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("=" * 70)
        logger.error(f"❌ EXTRACTION FAILED: {str(e)}")
        logger.error("=" * 70)
        raise