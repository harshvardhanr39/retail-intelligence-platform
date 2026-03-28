import os
import sys
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from pipeline.utils.metadata import start_run, complete_run, fail_run
from pipeline.loaders.s3_parquet_loader import write_to_bronze

load_dotenv()
logger = logging.getLogger(__name__)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=1, max=10),
    reraise=True
)

def fetch_products() -> list:
    """Fetch product data from FakestoreAPI with retries and return as a list."""
    print("🔍 Fetching products from FakestoreAPI...")
    resp = requests.get("https://fakestoreapi.com/products", timeout=10)
    resp.raise_for_status()
    return resp.json()


def main():
    source_name = "products"
    run_date = datetime.utcnow().date()
    s3_bucket = os.getenv("S3_BUCKET_NAME")

    #1. log start run
    run_id = start_run(source_name)

    try:
        # fetch with retries
        try:
            raw = fetch_products()
        except Exception as e:
            print("[products] FakeStoreAPI failed after 3 retries. Trying fallback...")
            resp=requests.get("https://dummyjson.com/products", timeout=10)
            data = resp.json()
            raw = [
                {
                    "id": p["id"], "title": p["title"],
                    "category": p.get("category", "general"),
                    "price": p["price"], "description": p.get("description", ""),
                    "image": p.get("thumbnail", ""),
                    "rating": {"rate": p.get("rating", 0), "count": p.get("stock", 0)}
                }
                for p in data.get("products", [])
            ]
        df = pd.DataFrame(raw)

        #flatten nested rating
        df['rating_rate'] = df['rating'].apply(lambda x: x.get('rate', 0) if isinstance(x, dict) else 0)
        df['rating_count'] = df['rating'].apply(lambda x: x.get('count', 0) if isinstance(x, dict) else 0)
        df.drop(columns=['rating'], inplace=True)

        #rename id to product_id for consistency
        df.rename(columns={'id': 'product_id', 'image': 'image_url'}, inplace=True)
        df['product_id'] = df['product_id'].astype(str)

        print(f"[{source_name}] Fetched {len(df):,} products")

        _,rows_written = write_to_bronze(df, source_name, run_date, s3_bucket)

        
        complete_run(run_id, len(df), rows_written)

    except Exception as e:
        print(f"[{source_name}] Error: {str(e)}")
        fail_run(run_id, str(e))
        raise

if __name__ == "__main__":
    main()
