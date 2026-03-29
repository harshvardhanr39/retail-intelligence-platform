import pandera as pa
from pandera import Column, DataFrameSchema, Check
import pandas as pd

orders_schema = DataFrameSchema(
    {
        "order_id": Column(
            str,
            nullable=False,
            unique=True,
            description="Unique order identifier"
        ),
        "customer_id": Column(
            str,
            nullable=False,
            description="Reference to customer"
        ),
        "status": Column(
            str,
            checks=Check.isin([
                "pending", "processing", "shipped",
                "delivered", "cancelled"
            ]),
            nullable=False,
            description="Order status"
        ),
        "total_amount": Column(
            float,
            checks=Check.greater_than_or_equal_to(0),
            nullable=False,
            description="Total order value"
        ),
        "created_at": Column(
            pa.dtypes.DateTime,
            nullable=False,
            description="Order creation timestamp"
        ),
    },
    coerce=True,
    strict=False,
    name="orders",
)

def validate_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate orders DataFrame against contract.
    lazy=True collects ALL violations before raising.
    """
    return orders_schema.validate(df, lazy=True)
