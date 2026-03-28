import os
import sys
import shutil
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import psycopg2

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from pipeline.utils.metadata import start_run, complete_run, fail_run
from pipeline.loaders.s3_parquet_loader import write_to_bronze

load_dotenv()

def main():
    source_name = "inventory"
    run_date = datetime.utcnow().date()
    s3_bucket = os.getenv("S3_BUCKET_NAME")
    landing_dir = 'data/landing/inventory'
    processed_dir = f'{landing_dir}/processed'
    today_file = f"{landing_dir}/inventory_{run_date}.csv"

    os.makedirs(processed_dir, exist_ok=True)

    #1. log start run
    run_id = start_run(source_name)

    try:
        #check if today's file exists in landing
        if not os.path.exists(today_file):
            print(f"[{source_name}] WARNING: No file found at {today_file}. Skipping.")
            complete_run(run_id, 0, 0)
            return
        
        print(f"[{source_name}] Found file: {today_file}. Proceeding with extraction.")

        #read csv file
        df = pd.read_csv(today_file)
        print(f"[{source_name}] Read {len(df):,} rows from {today_file}")

        #write to bronze layer in s3
        _, row_written = write_to_bronze(df, source_name, run_date, s3_bucket)
       
        #move processed file to processed dir
        dest = f"{processed_dir}/inventory_{run_date}.csv"
        shutil.move(today_file, dest)
        print(f"[{source_name}] Moved processed file to {dest}")
    
        #log complete run
        complete_run(run_id, len(df), row_written)
    
    except Exception as e:
        fail_run(run_id, str(e))
        raise

if __name__ == "__main__":
    main()
