import uuid
from datetime import datetime, date
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def start_run(source_name: str, run_date: date = None) -> str:
    """
    Log that a pipeline run has started.
    Returns run_id — pass this to complete_run or fail_run.
    """
    run_id = str(uuid.uuid4())
    run_date = run_date or datetime.utcnow().date()

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO pipeline_meta.pipeline_runs 
                    (run_id, source_name, run_date, started_at, status)
                VALUES 
                    (%s, %s, %s, NOW(), 'running')
                RETURNING run_id
            """, (run_id, source_name, run_date))
            run_id = cursor.fetchone()[0]
        conn.commit()

    print(f'[{source_name}] Starting run: {run_id}')
    return run_id

def complete_run(run_id: str,rows_extracted: int, rows_written: int):
    """
    Log that a pipeline run has completed successfully.
    """
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE pipeline_meta.pipeline_runs 
                SET 
                    status = 'success', 
                    finished_at = NOW(),
                    rows_extracted = %s,
                    rows_written = %s
                WHERE run_id = %s
            """, (rows_extracted, rows_written, run_id))
        conn.commit()

    print(f"[run {run_id}] ✅ Completed — extracted: {rows_extracted}, written: {rows_written}")


def fail_run(run_id: str, error_message: str):
    """
    Log that a pipeline run has failed.
    """
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE pipeline_meta.pipeline_runs 
                SET 
                    status = 'failed', 
                    finished_at = NOW(),
                    error_message = %s
                WHERE run_id = %s
            """, (str(error_message)[:2000], run_id))
        conn.commit()

    print(f"[run {run_id}] ❌ Failed with error: {error_message}")