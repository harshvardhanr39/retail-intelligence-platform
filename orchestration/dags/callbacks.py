import os
import requests

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")

def slack_alert(context):
    """Fires on any task failure."""
    dag_id        = context.get("dag").dag_id
    task_id       = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    exception     = context.get("exception")
    log_url       = context.get("task_instance").log_url

    message = {
        "text": (
            f"❌ *Pipeline Failed*\n"
            f"*DAG:* `{dag_id}`\n"
            f"*Task:* `{task_id}`\n"
            f"*Time:* {execution_date}\n"
            f"*Error:* {str(exception)[:500]}\n"
            f"*Logs:* {log_url}"
        )
    }

    if SLACK_WEBHOOK:
        try:
            requests.post(SLACK_WEBHOOK, json=message, timeout=10)
        except Exception as e:
            print(f"Failed to send Slack alert: {e}")
    else:
        print("WARNING: SLACK_WEBHOOK_URL not set — skipping alert")


def slack_success(context):
    """Fires when the entire DAG completes successfully."""
    dag_id         = context.get("dag").dag_id
    execution_date = context.get("execution_date")

    message = {
        "text": (
            f"✅ *Pipeline Completed Successfully*\n"
            f"*DAG:* `{dag_id}`\n"
            f"*Time:* {execution_date}"
        )
    }

    if SLACK_WEBHOOK:
        try:
            requests.post(SLACK_WEBHOOK, json=message, timeout=10)
        except Exception as e:
            print(f"Failed to send Slack success message: {e}")
