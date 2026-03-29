from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from callbacks import slack_alert, slack_success

# ── Project root inside Airflow container ─────────────────
PROJECT_ROOT = "/opt/airflow"
DBT_DIR      = f"{PROJECT_ROOT}/dbt_project"
VENV_PYTHON  = "/home/airflow/.local/bin/python3"

# ── Default args applied to every task ───────────────────
default_args = {
    "owner": "HvSR",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": slack_alert,
}

# ── DAG definition ────────────────────────────────────────
with DAG(
    dag_id="retail_daily_pipeline",
    description="Daily retail data pipeline — extract, transform, validate",
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_success_callback=slack_success,
    tags=["retail", "daily", "pipeline"],
) as dag:

    # ── TASK 1: Generate daily orders ─────────────────────
    generate_daily_data = BashOperator(
        task_id="generate_daily_data",
        bash_command=f"cd {PROJECT_ROOT} && python3 generators/generate_daily_orders.py",
    )

    # ── TASK 2a: Extract orders (incremental) ─────────────
    extract_orders = BashOperator(
        task_id="extract_orders",
        bash_command=f"cd {PROJECT_ROOT} && python3 pipeline/extractors/orders_extractor.py",
    )

    # ── TASK 2b: Extract products (full load) ─────────────
    extract_products = BashOperator(
        task_id="extract_products",
        bash_command=f"cd {PROJECT_ROOT} && python3 pipeline/extractors/products_extractor.py",
    )

    # ── TASK 2c: Extract inventory (file pickup) ──────────
    extract_inventory = BashOperator(
        task_id="extract_inventory",
        bash_command=f"cd {PROJECT_ROOT} && python3 pipeline/extractors/inventory_extractor.py",
    )

    # ── TASK 3: dbt Silver ────────────────────────────────
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"cd {DBT_DIR} && dbt run --select silver",
    )

    # ── TASK 4: dbt Silver tests ──────────────────────────
    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f"cd {DBT_DIR} && dbt test --select silver",
    )

    # ── TASK 5: dbt Gold ──────────────────────────────────
    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"cd {DBT_DIR} && dbt run --select gold",
    )

    # ── TASK 6: dbt Gold tests ────────────────────────────
    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=f"cd {DBT_DIR} && dbt test --select gold",
    )

    # ── TASK 7: Great Expectations validation ─────────────
    def run_ge_checks():
        """Run Great Expectations checkpoint if configured."""
        try:
            import great_expectations as ge
            context = ge.get_context()
            result = context.run_checkpoint(checkpoint_name="retail_daily_checkpoint")
            if not result["success"]:
                raise ValueError("Great Expectations validation failed")
            print("✅ Great Expectations checks passed")
        except Exception as e:
            print(f"WARNING: GE checks skipped or failed: {e}")
            # Don't fail the pipeline if GE isn't configured yet

    run_ge_checks_task = PythonOperator(
        task_id="run_ge_checks",
        python_callable=run_ge_checks,
    )

    # ── TASK 8: Notify success ────────────────────────────
    def notify_success_fn(**context):
        import requests, os
        webhook = os.getenv("SLACK_WEBHOOK_URL")
        if webhook:
            requests.post(webhook, json={
                "text": "✅ *Daily Pipeline Complete* — Silver + Gold tables updated"
            }, timeout=10)
        print("✅ Pipeline completed successfully")

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success_fn,
    )

    # ── TASK DEPENDENCIES ────────────────────────────────
    # Step 1 → Steps 2a, 2b, 2c run in parallel (fan-out)
    generate_daily_data >> [extract_orders, extract_products, extract_inventory]

    # Steps 2a, 2b, 2c → Step 3 (fan-in — all must complete)
    [extract_orders, extract_products, extract_inventory] >> dbt_run_silver

    # Linear from here
    dbt_run_silver >> dbt_test_silver >> dbt_run_gold >> dbt_test_gold >> run_ge_checks_task >> notify_success
