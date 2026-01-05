import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.dialects.postgresql import insert
import os
from datetime import datetime

DB_URL = "postgresql://airflow:airflow@host.docker.internal:5435/hackernews"

def load_parquet_to_postgres():
    today = datetime.now().strftime("%Y-%m-%d")
    input_path = f"/opt/airflow/data/processed/{today}/stories.parquet"

    if not os.path.exists(input_path):
        error_msg = f"✗ No parquet file found at {input_path}"
        print(error_msg)
        raise FileNotFoundError(error_msg)

    print(f"Reading {input_path}...")
    df = pd.read_parquet(input_path)
    
    print(f"✓ Loaded {len(df)} rows")
    print(f"Columns: {df.columns.tolist()}")

    engine = create_engine(DB_URL)

    print("Loading data to PostgreSQL...")
    try:
        # Get count before
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) as cnt FROM stories"))
            rows_before = result.fetchone()[0]
            print(f"Rows before insert: {rows_before}")
        
        # Reflect the existing table structure
        metadata = MetaData()
        stories_table = Table('stories', metadata, autoload_with=engine)
        
        # Insert with conflict handling
        inserted = 0
        skipped = 0
        
        with engine.begin() as conn:
            for _, row in df.iterrows():
                stmt = insert(stories_table).values(**row.to_dict())
                stmt = stmt.on_conflict_do_nothing(index_elements=['id'])
                result = conn.execute(stmt)
                if result.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
        
        print(f"✓ Success! Inserted {inserted} new rows, skipped {skipped} duplicates")
        
        # Get count after
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) as cnt FROM stories"))
            rows_after = result.fetchone()[0]
            print(f"Rows after insert: {rows_after}")
        
    except Exception as e:
        error_msg = f"✗ Error loading to Postgres: {e}"
        print(error_msg)
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    load_parquet_to_postgres()