from sqlalchemy import create_engine, text

DB_URL = "postgresql://airflow:airflow@host.docker.internal:5435/hackernews"

def init_tables():
    engine = create_engine(DB_URL)
    
    create_tables_sql = """
    CREATE TABLE IF NOT EXISTS stories (
        id BIGINT PRIMARY KEY,
        title TEXT,
        by VARCHAR(255),
        score INT,
        posted_at TIMESTAMP,
        hour_posted INT,
        domain VARCHAR(255),
        story_type VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS daily_stats (
        stat_date DATE PRIMARY KEY,
        total_stories INT,
        avg_score FLOAT,
        top_domain VARCHAR(255),
        most_active_author VARCHAR(255)
    );
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_tables_sql))
        conn.commit()
        print("Tables created successfully in hackernews database!")

if __name__ == "__main__":
    init_tables()