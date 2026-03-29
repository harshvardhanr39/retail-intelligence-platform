import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def log_quality_failures(source_name: str, failures_df, run_id: str = None):
    """
    Log contract violations to data_quality.quality_results table.
    Never raises — violations are logged but pipeline continues.
    """
    if failures_df is None or len(failures_df) == 0:
        return

    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        rows = []
        for _, row in failures_df.iterrows():
            rows.append((
                run_id,
                str(row.get("check", "unknown")),
                source_name,
                str(row.get("column", "")),
                "fail",
                1,
                None,
                str(row.get("failure_case", ""))[:500],
            ))

        cur.executemany("""
            INSERT INTO data_quality.quality_results
                (run_id, check_name, table_name, column_name,
                 result, failing_rows, total_rows, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, rows)

        conn.commit()
        cur.close()
        conn.close()
        print(f"[{source_name}] Logged {len(rows)} quality violations to DB")

    except Exception as e:
        print(f"[{source_name}] WARNING: Could not log quality failures: {e}")
