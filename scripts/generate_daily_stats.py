from sqlalchemy import create_engine, text
from datetime import datetime

DB_URL = "postgresql://airflow:airflow@host.docker.internal:5435/hackernews"

def generate_stats():
    try:
        engine = create_engine(DB_URL)
        today = datetime.now().strftime("%Y-%m-%d")
        
        print(f"Generating stats for {today}...")
        
        # Use begin() for transaction management
        with engine.begin() as conn:
            # First, make sure the daily_stats table exists
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS daily_stats (
                stat_date DATE PRIMARY KEY,
                total_stories INT,
                avg_score FLOAT,
                top_domain VARCHAR(255),
                most_active_author VARCHAR(255)
            );
            """
            conn.execute(text(create_table_sql))
            print("✓ daily_stats table ready")
            
            # Check if we have data for today
            count_check = f"SELECT COUNT(*) FROM stories WHERE DATE(posted_at) = '{today}'"
            result = conn.execute(text(count_check))
            count = result.fetchone()[0]
            
            if count == 0:
                print(f"⚠ No stories found for {today}, skipping stats generation")
                return
            
            print(f"✓ Found {count} stories for {today}")
            
            # Simple stats query
            stats_sql = f"""
            WITH daily_metrics AS (
                SELECT 
                    '{today}'::DATE as stat_date,
                    COUNT(*)::INT as total_stories,
                    ROUND(AVG(score)::numeric, 2)::FLOAT as avg_score
                FROM stories
                WHERE DATE(posted_at) = '{today}'
            ),
            top_domain_agg AS (
                SELECT domain
                FROM stories
                WHERE DATE(posted_at) = '{today}' 
                  AND domain IS NOT NULL 
                  AND domain != ''
                GROUP BY domain
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ),
            top_author_agg AS (
                SELECT by as author
                FROM stories
                WHERE DATE(posted_at) = '{today}'
                  AND by IS NOT NULL
                GROUP BY by
                ORDER BY COUNT(*) DESC
                LIMIT 1
            )
            INSERT INTO daily_stats (stat_date, total_stories, avg_score, top_domain, most_active_author)
            SELECT 
                m.stat_date,
                m.total_stories,
                m.avg_score,
                COALESCE((SELECT domain FROM top_domain_agg), 'N/A') as top_domain,
                COALESCE((SELECT author FROM top_author_agg), 'N/A') as most_active_author
            FROM daily_metrics m
            ON CONFLICT (stat_date) DO UPDATE SET
                total_stories = EXCLUDED.total_stories,
                avg_score = EXCLUDED.avg_score,
                top_domain = EXCLUDED.top_domain,
                most_active_author = EXCLUDED.most_active_author;
            """
            
            conn.execute(text(stats_sql))
            print(f"✓ Daily stats for {today} generated successfully!")
            
        # Query results outside the transaction
        with engine.connect() as conn:
            result_sql = f"SELECT * FROM daily_stats WHERE stat_date = '{today}'"
            result = conn.execute(text(result_sql))
            row = result.fetchone()
            print(f"Stats: Total={row[1]}, Avg Score={row[2]}, Top Domain={row[3]}, Top Author={row[4]}")
            
    except Exception as e:
        print(f"✗ Error generating stats: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    generate_stats()