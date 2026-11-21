"""
Weekly US housing news from GDELT, filtered in BigQuery (design doc D15):
BigQuery -> raw zone (rows + job cost) -> GE -> bronze.gdelt_housing_articles_raw.

Runs Monday mornings for the last complete ISO week (~3 GB scanned). Trigger
manually with start_date/end_date to backfill; windows are Monday-aligned
and each query is dry-run against a budget before it scans anything.

Auth: keyless ADC mounted read-only (docker/compose.yaml), impersonating the
BigQuery Job User service account. Needs GCP_PROJECT in .env.
"""

from datetime import date, datetime, timedelta

from airflow.sdk import Param, dag, task

from rpof_assets import BRONZE_GDELT


def _alert(context):
    from ingestion.alerts import task_failure_alert

    task_failure_alert(context)


@dag(
    dag_id="ingest_gdelt_news",
    schedule="0 8 * * 1",  # Mondays 08:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": _alert,
    },
    params={
        "start_date": Param(None, type=["null", "string"], format="date",
                            description="Backfill start. Empty = last full week."),
        "end_date": Param(None, type=["null", "string"], format="date",
                          description="Backfill end (exclusive). Empty = this Monday."),
    },  # fmt: skip
    tags=["gdelt", "ingest", "bigquery", "news"],
)
def ingest_gdelt_news():
    @task
    def extract(params=None, data_interval_end=None, dag_run=None) -> list[str]:
        from ingestion.gdelt_pipeline import extract_to_raw, last_complete_week

        today = (data_interval_end or dag_run.run_after).date()
        start, end = last_complete_week(today)
        if params.get("start_date"):
            start = date.fromisoformat(params["start_date"])
        if params.get("end_date"):
            end = date.fromisoformat(params["end_date"])
        return extract_to_raw(start, end)

    @task(outlets=[BRONZE_GDELT])
    def load_bronze(keys: list[str], run_id=None) -> dict:
        from ingestion.gdelt_pipeline import load_raw_to_bronze

        return load_raw_to_bronze(keys, run_id=run_id)

    load_bronze(extract())


ingest_gdelt_news()
