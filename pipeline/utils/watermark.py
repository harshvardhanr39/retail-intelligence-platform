from datetime import datetime, timedelta
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def get_watermark(source_name: str) -> datetime:
    """
    get the last processed timestamp for a given source
    """
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT last_watermark 
                FROM pipeline_meta.watermarks 
                WHERE source_name = %s
            """, (source_name,))
            row = cursor.fetchone()

    if row:
        print(f'[{source_name}] Current watermark: {row[0]}')
        return row[0]
    
    # First ever run, go back to 90 days
    default = datetime.utcnow() - timedelta(days=90)
    print(f'[{source_name}] No watermark found, defaulting to: {default}')
    return default

def update_watermark(source_name: str, new_watermark: datetime):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_meta.watermarks
                    (source_name, last_run_at, last_watermark, updated_at)
                VALUES
                    (%s, NOW(), %s, NOW())
                ON CONFLICT (source_name) DO UPDATE
                SET
                    last_run_at    = NOW(),
                    last_watermark = EXCLUDED.last_watermark,
                    updated_at     = NOW()
            """, (source_name, new_watermark))
        conn.commit()
    print(f"[{source_name}] Watermark updated to: {new_watermark}")
 

