# Hacker News Data Pipeline

A production-grade ETL pipeline that extracts, transforms, and loads Hacker News stories daily. Built to demonstrate real-world data engineering practices with modern tools.

## What It Does

This pipeline automatically collects 500+ stories from Hacker News every day, processes them with Apache Spark, stores everything in PostgreSQL, and visualizes insights through a dashboard. Raw data is backed up to AWS S3 for safety.

**Why I built this:** To learn how real data pipelines work in production - handling failures, ensuring data quality, and making everything reproducible.

---

## Architecture
```
Hacker News API
       ↓
   Extract (Python)
   - Retry logic
   - Rate limiting
   - Quality checks
       ↓
   ┌───────────┬──────────┐
   ↓           ↓          ↓
Local Files  AWS S3   Transform (Spark)
                      - Clean data
                      - Add features
                      - Save as Parquet
                          ↓
                    Load (PostgreSQL)
                    - Upsert logic
                    - Schema validation
                          ↓
                    Analytics Layer
                    - Daily stats
                    - Aggregations
                          ↓
                    Streamlit Dashboard

    All orchestrated by Apache Airflow
```

The pipeline runs daily at 8 AM UTC. Each step validates data quality before moving forward.

---

## Screenshots

### Airflow DAG View
![Airflow DAG](screenshots/airflow_dag.png)
*The complete pipeline with all tasks and dependencies*

### S3 Data Lake
![S3 Storage](screenshots/s3_bucket.png)
*Raw data organized by date in AWS S3*

### Analytics Dashboard
![Dashboard Overview](screenshots/dashboard_main.png)
*Real-time insights and trends*

![Dashboard Charts](screenshots/dashboard_charts.png)
*Story volume and posting time analysis*

---

## Tech Stack

- **Orchestration:** Apache Airflow - schedules and monitors the whole pipeline
- **Processing:** Apache Spark (PySpark) - handles data transformations
- **Storage:** AWS S3 (raw data backup), PostgreSQL (analytics), Local files (processing)
- **Visualization:** Streamlit dashboard
- **Deployment:** Docker Compose for everything

---

## Key Features

**Pipeline Reliability**
- Automatic retries with exponential backoff when APIs fail
- Data quality checks at every stage (extraction, transformation, loading)
- Idempotent - safe to run multiple times without duplicating data
- Comprehensive logging for debugging

**Data Processing**
- Fetches 500+ stories daily from HN's top, new, and best endpoints
- Removes duplicates and invalid records
- Extracts domains from URLs, categorizes story types (Show HN, Ask HN, etc.)
- Parses timestamps to analyze posting patterns
- Handles missing data gracefully

**Storage Strategy**
- Raw JSON backed up to S3 (disaster recovery)
- Processed Parquet files stored locally (fast access)
- PostgreSQL for analytics (structured queries)

**Analytics**
- Daily statistics: story counts, average scores, top domains
- Interactive dashboard showing trends and insights
- SQL-ready for custom analysis

---

## Project Structure
```
hn-pipeline/
├── dags/
│   └── hn_pipeline_dag.py          # Airflow workflow definition
├── scripts/
│   ├── extract_stories.py          # Fetch data from HN API
│   ├── transform_spark.py          # Spark transformations
│   ├── load_to_postgres.py         # Load into database
│   ├── generate_daily_stats.py     # Create aggregations
│   └── data_quality_checks.py      # Validation logic
├── data/
│   ├── raw/                        # JSON files by date
│   └── processed/                  # Parquet files by date
├── screenshots/                    # Project screenshots
├── dashboard.py                    # Streamlit visualization
├── docker-compose.yaml             # Service definitions
├── Dockerfile                      # Airflow + dependencies
└── requirements.txt                # Dashboard dependencies
```

---

## Getting Started

### Prerequisites

- Docker Desktop (8GB RAM minimum)
- AWS account (optional, for S3 backup)
- 10GB free disk space

### Setup

**1. Clone and configure**
```bash
git clone https://github.com/yourusername/hn-pipeline.git
cd hn-pipeline

# Set up environment variables
cp .env.example .env
# Edit .env with your AWS credentials (optional)
```

**2. Start the services**
```bash
docker-compose build
docker-compose up -d
```

**3. Initialize Airflow**
```bash
# Set up Airflow database
docker-compose run --rm airflow-webserver airflow db migrate

# Create admin user
docker-compose run --rm airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

**4. Create database tables**
```bash
# Connect to Postgres
docker exec -it <postgres-container-name> psql -U airflow

# Run these SQL commands:
CREATE DATABASE hackernews;
\c hackernews

CREATE TABLE stories (
    id BIGINT PRIMARY KEY,
    title TEXT,
    by VARCHAR(255),
    score INT,
    posted_at TIMESTAMP,
    hour_posted INT,
    domain VARCHAR(255),
    story_type VARCHAR(50)
);

CREATE TABLE daily_stats (
    stat_date DATE PRIMARY KEY,
    total_stories INT,
    avg_score FLOAT,
    top_domain VARCHAR(255),
    most_active_author VARCHAR(255)
);
```

**5. Run the pipeline**

- Go to http://localhost:8085 (username: admin, password: admin)
- Find `hn_daily_pipeline` and toggle it ON
- Click the play button to trigger manually
- Takes about 4 minutes to complete

**6. View the dashboard**
```bash
pip install -r requirements.txt
streamlit run dashboard.py
```

Open http://localhost:8501

---

## Data Quality Checks

The pipeline validates data at every step:

**After extraction:**
- At least 400 stories fetched
- No duplicate IDs
- All required fields present (id, title, author, score, timestamp)
- Scores within reasonable range (< 10,000)
- Stories are recent (within 48 hours)

**After transformation:**
- Row count hasn't dropped suspiciously
- Critical columns have no nulls
- Timestamps parsed correctly
- Story types categorized properly
- Domain extraction worked

**After loading:**
- Database has expected number of rows
- No duplicate IDs in the database
- Data distributed correctly across dates

If any check fails, the pipeline stops and logs the issue.

---

## Example Queries
```sql
-- Top stories this week
SELECT title, score, by as author, domain
FROM stories
WHERE posted_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY score DESC
LIMIT 10;

-- Best time to post
SELECT hour_posted, COUNT(*) as posts, AVG(score) as avg_score
FROM stories
GROUP BY hour_posted
ORDER BY avg_score DESC;

-- Most popular domains
SELECT domain, COUNT(*) as stories, AVG(score) as avg_score
FROM stories
WHERE domain IS NOT NULL
GROUP BY domain
ORDER BY stories DESC
LIMIT 15;
```

---

## What I Learned

Building this taught me:
- How to design resilient data pipelines that handle failures gracefully
- When to use Spark vs Pandas (and why Spark matters at scale)
- Proper error handling, retry logic, and idempotency patterns
- Data quality frameworks and why they're critical
- Orchestration with Airflow (DAGs, task dependencies, scheduling)
- Working with cloud storage (S3), databases (PostgreSQL), and containers (Docker)
- The importance of logging and monitoring

---

## Limitations & Future Work

**Current limitations:**
- Processed data stored locally only (not backed up to S3)
- Single-instance deployment (no high availability)
- Runs once daily (not real-time)

**Next steps:**
- Upload processed Parquet files to S3
- Add dbt for more sophisticated data modeling
- Implement incremental loading (only fetch new stories)
- Set up monitoring and alerting
- Add sentiment analysis on titles
- Deploy to AWS with proper CI/CD

---

## Troubleshooting

**Pipeline not running?**
- Check `docker-compose logs airflow-scheduler` for errors
- Make sure all containers are up: `docker-compose ps`
- Verify .env file has correct credentials

**Dashboard shows no data?**
- Run the pipeline at least once first
- Check database: `docker exec -it <postgres-container> psql -U airflow -d hackernews -c "SELECT COUNT(*) FROM stories;"`

**S3 upload failing?**
- Verify AWS credentials in .env
- Check bucket exists: `aws s3 ls s3://your-bucket-name`
- Pipeline will continue even if S3 fails (data stays local)


Built by Mahmou Gribej | [LinkedIn](www.linkedin.com/in/mahmoud-gribej-70bb24265) | [Email](mahmoudgribej7@gmail.com)
