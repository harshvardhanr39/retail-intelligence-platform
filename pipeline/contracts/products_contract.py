from pandera import Column, DataFrameSchema, Check
import pandas as pd

products_schema = DataFrameSchema(
    {
        "product_id": Column(
            str,
            nullable=False,
            unique=True,
        ),
        "title": Column(
            str,
            nullable=False,
        ),
        "price": Column(
            float,
            checks=Check.greater_than(0),
            nullable=False,
        ),
        "category": Column(
            str,
            nullable=True,
        ),
    },
    coerce=True,
    strict=False,
    name="products",
)

def validate_products(df: pd.DataFrame) -> pd.DataFrame:
    return products_schema.validate(df, lazy=True)
