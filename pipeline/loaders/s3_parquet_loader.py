import os
import io
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def write_to_bronze(
    df: pd.DataFrame,
    source_name: str,
    run_date,
    s3_bucket: str
) -> tuple:

    if df.empty:
        print(f"[{source_name}] DataFrame is empty — nothing to write")
        return None, 0

    # Add metadata columns
    df = df.copy()
    df["_ingested_at"] = datetime.utcnow()
    df["_source"]      = source_name
    df["_run_date"]    = str(run_date)

    # Parse run_date
    if isinstance(run_date, str):
        date_obj = datetime.strptime(run_date, "%Y-%m-%d")
    else:
        date_obj = run_date

    # S3 key — path inside the bucket (no bucket name, no s3://)
    s3_key = (
        f"bronze/{source_name}"
        f"/year={date_obj.year}"
        f"/month={date_obj.month:02d}"
        f"/day={date_obj.day:02d}"
        f"/{source_name}_{run_date}.parquet"
    )

    # Convert DataFrame to Parquet bytes in memory
    table = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    # Upload to S3 using boto3 directly
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-1")
    )

    s3_client.put_object(
        Bucket=s3_bucket,       # just the bucket name
        Key=s3_key,             # just the path inside the bucket
        Body=buffer.getvalue()
    )

    row_count = len(df)
    print(f"[{source_name}] ✅ Written {row_count:,} rows → s3://{s3_bucket}/{s3_key}")
    return f"s3://{s3_bucket}/{s3_key}", row_count