from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Add scripts folder to Python path
sys.path.append('/opt/airflow/scripts')

from extract_stories import main as extract_main
from transform_spark import run_spark_transform
from load_to_postgres import load_parquet_to_postgres
from generate_daily_stats import generate_stats

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hn_daily_pipeline',
    default_args=default_args,
    description='Hacker News ETL Pipeline',
    schedule_interval='0 8 * * *',  # Daily at 8 AM
    catchup=False,
)

extract_task = PythonOperator(
    task_id='extract_hn_stories',
    python_callable=extract_main,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_with_spark',
    python_callable=run_spark_transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_parquet_to_postgres,
    dag=dag,
)

stats_task = PythonOperator(
    task_id='generate_daily_stats',
    python_callable=generate_stats,
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task >> stats_task