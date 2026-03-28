import os
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import psycopg2

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from pipeline.utils.watermark import get_watermark, update_watermark
from pipeline.utils.metadata import start_run, complete_run, fail_run
from pipeline.loaders.s3_parquet_loader import write_to_bronze

load_dotenv()

def main():
    source_name = "orders"
    run_date = datetime.utcnow().date()
    s3_bucket = os.getenv("S3_BUCKET_NAME")

    #1. log start run
    run_id = start_run(source_name)

    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))

        #2. get last watermark
        watermark = get_watermark(source_name)
        print(f"[{source_name}] Extracting data since: {watermark}")

        #3. extract new data since last watermark
        orders_sql = f"""
            SELECT
                o.order_id, o.customer_id, o.status, o.currency,
                o.subtotal, o.discount_amount, o.tax_amount,
                o.shipping_amount, o.total_amount,
                o.created_at, o.updated_at,
                c.email AS customer_email,
                c.segment AS customer_segment
            FROM source_data.orders o
            JOIN source_data.customers c
                ON o.customer_id = c.customer_id
            WHERE o.updated_at > '{watermark}'
            ORDER BY o.updated_at ASC
        """

        df_orders = pd.read_sql(orders_sql, conn)
        print(f"[{source_name}] Extracted {len(df_orders):,} orders")

        if df_orders.empty:
            print(f"[{source_name}] No new orders since {watermark}. Exiting.")
            complete_run(run_id, 0, 0)
            conn.close()
            return

        # 4. Extract order items for the same period
        items_sql = f"""
            SELECT oi.*
            FROM source_data.order_items oi
            JOIN source_data.orders o ON oi.order_id = o.order_id
            WHERE o.updated_at > '{watermark}'
        """
        df_items = pd.read_sql(items_sql, conn)
        print(f"[{source_name}] Extracted {len(df_items):,} order items")
        conn.close()

        #5. write to bronze layer
        _, orders_written = write_to_bronze(df_orders, "orders", run_date, s3_bucket)
        _, items_written = write_to_bronze(df_items, "order_items", run_date, s3_bucket)

        #6. update watermark to latest updated_at
        new_watermark = df_orders['updated_at'].max()
        update_watermark(source_name, new_watermark)

        #7. log complete run
        complete_run(run_id, len(df_orders), orders_written)
    
    except Exception as e:
        print(f"[{source_name}] Error: {str(e)}")
        fail_run(run_id, str(e))
        raise

if __name__ == "__main__":
    main()
