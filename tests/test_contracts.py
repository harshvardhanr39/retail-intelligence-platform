import pytest
import pandas as pd
import pandera as pa
from pipeline.contracts.orders_contract import validate_orders
from pipeline.contracts.products_contract import validate_products
from pipeline.contracts.inventory_contract import validate_inventory

# ── Orders contract tests ─────────────────────────────────
class TestOrdersContract:

    def test_valid_orders_pass(self, sample_orders_df):
        result = validate_orders(sample_orders_df)
        assert len(result) == len(sample_orders_df)

    def test_null_customer_id_fails(self):
        bad_df = pd.DataFrame({
            "order_id":     ["order-1"],
            "customer_id":  [None],           # ← should fail
            "status":       ["delivered"],
            "total_amount": [100.0],
            "created_at":   pd.to_datetime(["2024-01-01"]),
        })
        with pytest.raises(pa.errors.SchemaErrors):
            validate_orders(bad_df)

    def test_invalid_status_fails(self):
        bad_df = pd.DataFrame({
            "order_id":     ["order-1"],
            "customer_id":  ["cust-1"],
            "status":       ["returned"],      # ← not in allowed values
            "total_amount": [100.0],
            "created_at":   pd.to_datetime(["2024-01-01"]),
        })
        with pytest.raises(pa.errors.SchemaErrors):
            validate_orders(bad_df)

    def test_negative_amount_fails(self):
        bad_df = pd.DataFrame({
            "order_id":     ["order-1"],
            "customer_id":  ["cust-1"],
            "status":       ["delivered"],
            "total_amount": [-50.0],           # ← negative
            "created_at":   pd.to_datetime(["2024-01-01"]),
        })
        with pytest.raises(pa.errors.SchemaErrors):
            validate_orders(bad_df)

    def test_duplicate_order_id_fails(self):
        bad_df = pd.DataFrame({
            "order_id":     ["order-1", "order-1"],  # ← duplicate
            "customer_id":  ["cust-1", "cust-2"],
            "status":       ["delivered", "pending"],
            "total_amount": [100.0, 200.0],
            "created_at":   pd.to_datetime(["2024-01-01", "2024-01-02"]),
        })
        with pytest.raises(pa.errors.SchemaErrors):
            validate_orders(bad_df)

    def test_null_order_id_fails(self):
        bad_df = pd.DataFrame({
            "order_id":     [None],
            "customer_id":  ["cust-1"],
            "status":       ["delivered"],
            "total_amount": [100.0],
            "created_at":   pd.to_datetime(["2024-01-01"]),
        })
        with pytest.raises(pa.errors.SchemaErrors):
            validate_orders(bad_df)


# ── Products contract tests ───────────────────────────────
class TestProductsContract:

    def test_valid_products_pass(self, sample_products_df):
        result = validate_products(sample_products_df)
        assert len(result) == len(sample_products_df)

    def test_zero_price_fails(self):
        bad_df = pd.DataFrame({
            "product_id": ["1"],
            "title":      ["Product A"],
            "price":      [0.0],           # ← must be > 0
            "category":   ["electronics"],
        })
        with pytest.raises(pa.errors.SchemaErrors):
            validate_products(bad_df)

    def test_null_title_fails(self):
        bad_df = pd.DataFrame({
            "product_id": ["1"],
            "title":      [None],          # ← not null
            "price":      [29.99],
            "category":   ["electronics"],
        })
        with pytest.raises(pa.errors.SchemaErrors):
            validate_products(bad_df)


# ── Inventory contract tests ──────────────────────────────
class TestInventoryContract:

    def test_valid_inventory_passes(self, sample_inventory_df):
        result = validate_inventory(sample_inventory_df)
        assert len(result) == len(sample_inventory_df)

    def test_null_sku_id_fails(self):
        bad_df = pd.DataFrame({
            "sku_id":           [None],
            "product_id":       ["1"],
            "warehouse_id":     ["WH-01"],
            "quantity_on_hand": [100],
            "reorder_point":    [20],
        })
        with pytest.raises(pa.errors.SchemaErrors):
            validate_inventory(bad_df)
