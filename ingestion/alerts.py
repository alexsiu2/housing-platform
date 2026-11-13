"""
Failure alerts (design doc §10.1: failures alert, never pass silently).

If ALERT_WEBHOOK_URL is set, alerts are POSTed as {"text": ...} (Slack
incoming-webhook format, also accepted by many chat tools). Otherwise they
are logged at ERROR level, which still shows in the Airflow task log.
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)


def send_alert(text: str) -> bool:
    """Send one alert. Returns True if delivered to the webhook."""
    url = os.getenv("ALERT_WEBHOOK_URL")
    logger.error("ALERT: %s", text)
    if not url:
        return False
    try:
        response = requests.post(url, json={"text": text}, timeout=15)
        response.raise_for_status()
        return True
    except requests.RequestException as e:
        # Never let alerting failures mask the original problem.
        logger.error("Could not deliver alert to webhook: %s", type(e).__name__)
        return False


def task_failure_alert(context: dict) -> None:
    """Airflow on_failure_callback: one line naming the DAG, task and run."""
    ti = context.get("task_instance") or context.get("ti")
    dag_id = getattr(ti, "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    run_id = context.get("run_id", "?")
    error = context.get("exception")
    reason = f"{type(error).__name__}: {str(error)[:200]}" if error else "failed"
    send_alert(f"[rpof] {dag_id}.{task_id} failed in {run_id}: {reason}")
