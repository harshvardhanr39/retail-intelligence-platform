import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://retail:retail_secret@localhost:5432/retail")

context = gx.get_context()

# Add datasource
datasource_config = {
    "name": "retail_postgres",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": DATABASE_URL,
    },
    "data_connectors": {
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "include_schema_name": True,
        }
    }
}

context.add_datasource(**datasource_config)
print("✅ Datasource added")

# Create suite
suite_name = "gold.daily_revenue.critical"
suite = context.add_expectation_suite(expectation_suite_name=suite_name)

batch_request = BatchRequest(
    datasource_name="retail_postgres",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="gold.mart_daily_revenue",
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name,
)

validator.expect_column_values_to_not_be_null("date")
validator.expect_column_values_to_be_unique("date")
validator.expect_column_values_to_be_between("total_revenue", min_value=0, max_value=1_000_000)
validator.expect_column_values_to_be_between("order_count", min_value=1, max_value=10_000)
validator.expect_column_values_to_not_be_null("avg_order_value")
validator.expect_table_row_count_to_be_between(min_value=1, max_value=365)
validator.expect_column_values_to_not_be_null("total_revenue")

validator.save_expectation_suite(discard_failed_expectations=False)
print(f"✅ Suite saved: {suite_name}")

# Create checkpoint
checkpoint_config = {
    "name": "retail_daily_checkpoint",
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": {
                "datasource_name": "retail_postgres",
                "data_connector_name": "default_inferred_data_connector_name",
                "data_asset_name": "gold.mart_daily_revenue",
            },
            "expectation_suite_name": suite_name,
        }
    ],
}

context.add_checkpoint(**checkpoint_config)
print("✅ Checkpoint created")

# Run checkpoint
print("\n🔍 Running checkpoint...")
result = context.run_checkpoint(checkpoint_name="retail_daily_checkpoint")

if result["success"]:
    print("✅ All expectations PASSED")
else:
    print("❌ Some expectations FAILED")

# Build docs
context.build_data_docs()
print("\n📊 Data Docs built:")
print("   gx/uncommitted/data_docs/local_site/index.html")
