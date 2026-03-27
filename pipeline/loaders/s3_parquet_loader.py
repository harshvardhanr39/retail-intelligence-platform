import os
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def write_to_bronze(
    df: pd.DataFrame, 
    source_name:str,  
    s3_bucket: str, 
    run_date: str
):
    """write dataframe to bronze layer in parquet format
    Returns :
    (s3_path and row_count)
    """
    if df.empty:
        print("⚠️ No data to write to bronze layer.")
        return None, 0

    df = df.copy()
    df['_ingestion_at'] = datetime.utcnow()
    df['_source'] = source_name
    df['_run_date'] = str(run_date)

    #build s3 path
    date_obj = pd.to_datetime(run_date)
    s3_path = (
        f"s3://{s3_bucket}/bronze/{source_name}"and
        f"/year={date_obj.year}"
        f"/month={date_obj.month:02d}"
        f"/day={date_obj.day:02d}/"
        f"{source_name}_{run_date}.parquet"
    )

    #write to s3 using s3fs

    fs = s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
        client_kwargs={"region_name": os.getenv("AWS_DEFAULT_REGION")},
    )

    table = pa.Table.from_pandas(df)

    with fs.open(s3_path, 'wb') as f:
        pq.write_table(table, f, compression='snappy')

    row_count = len(df)
    print(f"[{source_name}] ✅ Written {row_count:,} rows to {s3_path}")
    return s3_path, row_count

