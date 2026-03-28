import os
import sys
import json
import shutil
import glob
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from pipeline.utils.metadata import start_run, complete_run, fail_run
from pipeline.loaders.s3_parquet_loader import write_to_bronze

load_dotenv()

def main():
    source_name = "events"
    run_date = datetime.utcnow().date()
    s3_bucket = os.getenv("S3_BUCKET_NAME")
    landing_dir = "data/landing/events/"
    processed_dir = f"{landing_dir}/processed"

    os.makedirs(processed_dir, exist_ok=True)

    #1. log start run
    run_id = start_run(source_name)

    try:
        #2. read all json files from landing
        json_files = glob.glob(os.path.join(landing_dir, "*.json"))

        if not json_files:
            print(f"[{source_name}] No new event files found in landing. Exiting.")
            complete_run(run_id, 0, 0)
            return
        
        print(f"[{source_name}] Found {len(json_files)} files to process.")

        all_events = []
        bad_lines = 0
        
        for file in json_files:
            print(f"[{source_name}] Processing file: {file}")
            with open(file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                        all_events.append(event)
                    except json.JSONDecodeError:
                        bad_lines += 1
                        print(f"⚠️ Skipping malformed line in {file}: {line.strip()}")

        if not all_events:
            print(f"[{source_name}] No valid events extracted from files. Exiting.")
            complete_run(run_id, 0, bad_lines)
            return

        print(f"[{source_name}] Extracted {len(all_events)} events with {bad_lines} bad lines.")   

        df_events = pd.DataFrame(all_events)
        df_events['timestamp'] = pd.to_datetime(df_events['timestamp'], utc=True)

        #3. write to bronze layer
        _, rows_written = write_to_bronze(df_events, source_name, run_date, s3_bucket)

        #4. move processed files to processed dir
        for filepath in json_files:
            file_name = os.path.basename(filepath)
            shutil.move(filepath, f'{processed_dir}/{file_name}')
        print(f"[{source_name}] Moved processed files to {processed_dir}")

        #5. log complete run
        complete_run(run_id, len(json_files), rows_written)
    
    except Exception as e:
        fail_run(run_id, str(e))
        raise

if __name__ == "__main__":
    main()
