"""
FEMA National Risk Index: expected annual natural-hazard building loss by
census tract -> raw zone -> GE -> bronze.fema_nri_raw (context on the
dashboard, not a score input).

FEMA releases NRI about once a year (v1.20 in December 2025), so this runs in
February and can be triggered by hand. A release already loaded merges
idempotently; a new release adds rows (keyed on tract + release date).
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

from rpof_assets import BRONZE_FEMA_NRI


def _alert(context):
    from ingestion.alerts import task_failure_alert

    task_failure_alert(context)


@dag(
    dag_id="ingest_fema_nri",
    schedule="0 7 1 2 *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": _alert,
    },
    tags=["hazard", "ingest", "fema"],
)
def ingest_fema_nri():
    @task
    def extract() -> str:
        from ingestion.nri_pipeline import extract_to_raw

        return extract_to_raw()

    @task(outlets=[BRONZE_FEMA_NRI])
    def load_bronze(key: str, run_id=None) -> dict:
        from ingestion.nri_pipeline import load_raw_to_bronze

        return load_raw_to_bronze(key, run_id=run_id)

    load_bronze(extract())


ingest_fema_nri()
