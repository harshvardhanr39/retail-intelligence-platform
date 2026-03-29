import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import MagicMock
import os

# ── Set default env vars for tests ───────────────────────
os.environ.setdefault("DATABASE_URL", "postgresql://retail:retail_secret@localhost:5432/retail")
os.environ.setdefault("S3_BUCKET_NAME", "test-bucket")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test_key")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test_secret")
os.environ.setdefault("AWS_DEFAULT_REGION", "eu-west-1")

# ── Sample DataFrames ─────────────────────────────────────
@pytest.fixture
def sample_orders_df():
    return pd.DataFrame({
        "order_id":    ["order-1", "order-2", "order-3"],
        "customer_id": ["cust-1",  "cust-2",  "cust-1"],
        "status":      ["delivered", "pending", "shipped"],
        "total_amount": [150.00, 89.99, 230.50],
        "created_at":  pd.to_datetime([
            "2024-01-01", "2024-01-02", "2024-01-03"
        ]),
        "updated_at":  pd.to_datetime([
            "2024-01-02", "2024-01-02", "2024-01-03"
        ]),
    })

@pytest.fixture
def sample_products_df():
    return pd.DataFrame({
        "product_id": ["1", "2", "3"],
        "title":      ["Product A", "Product B", "Product C"],
        "category":   ["electronics", "jewelery", "electronics"],
        "price":      [29.99, 149.99, 999.99],
    })

@pytest.fixture
def sample_inventory_df():
    return pd.DataFrame({
        "sku_id":           ["SKU-1-WH-01", "SKU-2-WH-02"],
        "product_id":       ["1", "2"],
        "product_name":     ["Product A", "Product B"],
        "warehouse_id":     ["WH-01", "WH-02"],
        "quantity_on_hand": [100, 50],
        "reorder_point":    [20, 10],
        "supplier_code":    ["SUP-A1", "SUP-B2"],
        "last_count_date":  pd.to_datetime(["2024-01-01", "2024-01-02"]),
    })

# ── Mock DB connection ────────────────────────────────────
@pytest.fixture
def mock_db_conn(mocker):
    mock_conn   = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__  = MagicMock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
    mocker.patch("psycopg2.connect", return_value=mock_conn)
    return mock_conn, mock_cursor

# ── Mock S3 client ────────────────────────────────────────
@pytest.fixture
def mock_s3(mocker):
    mock_client = MagicMock()
    mock_client.put_object.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
    mocker.patch("boto3.client", return_value=mock_client)
    return mock_client
