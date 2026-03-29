from pandera import Column, DataFrameSchema
import pandas as pd

inventory_schema = DataFrameSchema(
    {
        "sku_id": Column(
            str,
            nullable=False,
        ),
        "product_id": Column(
            str,
            nullable=False,
        ),
        "warehouse_id": Column(
            str,
            nullable=False,
        ),
        "quantity_on_hand": Column(
            int,
            nullable=False,
            # NOTE: we allow negatives here intentionally
            # the Silver layer fixes them
            # the contract just flags them
        ),
        "reorder_point": Column(
            int,
            nullable=False,
        ),
    },
    coerce=True,
    strict=False,
    name="inventory",
)

def validate_inventory(df: pd.DataFrame) -> pd.DataFrame:
    return inventory_schema.validate(df, lazy=True)
