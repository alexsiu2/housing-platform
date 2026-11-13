"""
Weekly FRED ingest: API -> raw zone -> Great Expectations -> bronze.fred_series_raw.

  extract      fetch every series, land the untouched JSON in s3://rpof-raw/fred/
  load_bronze  parse the landed files, run the GE suite, MERGE into Bronze Delta

A scheduled run re-fetches the trailing `lookback_days` (FRED revises recent
data; the MERGE makes overlap idempotent). To backfill, trigger manually with
`start_date` / `end_date`, or run `make backfill-fred` from the host.

A successful load updates the bronze FRED asset, which schedules dbt_build.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task

from rpof_assets import BRONZE_FRED


def _alert(context):
    from ingestion.alerts import task_failure_alert

    task_failure_alert(context)


@dag(
    dag_id="ingest_fred",
    schedule="0 6 * * 1",  # Mondays 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": _alert,
    },
    params={
        "start_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Backfill start (YYYY-MM-DD). Empty = end - lookback_days.",
        ),
        "end_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Backfill end (YYYY-MM-DD). Empty = the run's date.",
        ),
        "lookback_days": Param(730, type="integer", minimum=1),
    },
    tags=["phase-1", "ingest", "fred"],
)
def ingest_fred():
    @task
    def extract(params=None, data_interval_end=None, dag_run=None) -> list[str]:
        from ingestion.fred_pipeline import extract_to_raw, resolve_window

        anchor = data_interval_end or dag_run.run_after
        start, end = resolve_window(
            params.get("start_date"),
            params.get("end_date"),
            lookback_days=params["lookback_days"],
            today=anchor.date(),
        )
        return extract_to_raw(start, end)

    @task(outlets=[BRONZE_FRED])
    def load_bronze(keys: list[str], run_id=None) -> dict:
        from ingestion.fred_pipeline import load_raw_to_bronze

        return load_raw_to_bronze(keys, run_id=run_id)

    load_bronze(extract())


ingest_fred()
