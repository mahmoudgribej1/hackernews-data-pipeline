from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/scripts')

from extract_stories import main as extract_main
from transform_spark import run_spark_transform
from load_to_postgres import load_parquet_to_postgres
from generate_daily_stats import generate_stats
from data_quality_checks import (
    run_extraction_checks, 
    run_transformation_checks,
    run_load_checks
)
from sqlalchemy import create_engine

DB_URL = "postgresql://airflow:airflow@host.docker.internal:5435/hackernews"

def check_extraction_quality(**context):
    """Quality checks after extraction"""
    today = datetime.now().strftime("%Y-%m-%d")
    data_path = f"/opt/airflow/data/raw/{today}/stories.json"
    run_extraction_checks(data_path)

def check_transformation_quality(**context):
    """Quality checks after transformation"""
    today = datetime.now().strftime("%Y-%m-%d")
    parquet_path = f"/opt/airflow/data/processed/{today}/stories.parquet"
    run_transformation_checks(parquet_path)

def check_load_quality(**context):
    """Quality checks after loading"""
    engine = create_engine(DB_URL)
    run_load_checks(engine)

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hn_daily_pipeline',
    default_args=default_args,
    description='Hacker News ETL Pipeline with Data Quality Checks',
    schedule_interval='0 8 * * *',
    catchup=False,
    tags=['data-engineering', 'etl', 'production'],
)

# Extract
extract_task = PythonOperator(
    task_id='extract_hn_stories',
    python_callable=extract_main,
    dag=dag,
)

# Quality Check 1
quality_check_extraction = PythonOperator(
    task_id='quality_check_extraction',
    python_callable=check_extraction_quality,
    dag=dag,
)

# Transform
transform_task = PythonOperator(
    task_id='transform_with_spark',
    python_callable=run_spark_transform,
    dag=dag,
)

# Quality Check 2
quality_check_transformation = PythonOperator(
    task_id='quality_check_transformation',
    python_callable=check_transformation_quality,
    dag=dag,
)

# Load
load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_parquet_to_postgres,
    dag=dag,
)

# Quality Check 3
quality_check_load = PythonOperator(
    task_id='quality_check_load',
    python_callable=check_load_quality,
    dag=dag,
)

# Stats
stats_task = PythonOperator(
    task_id='generate_daily_stats',
    python_callable=generate_stats,
    dag=dag,
)

# Define dependencies with quality checks
extract_task >> quality_check_extraction >> transform_task >> quality_check_transformation >> load_task >> quality_check_load >> stats_task