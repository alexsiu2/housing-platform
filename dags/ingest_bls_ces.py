"""
Monthly BLS CES metro employment: API -> raw zone -> GE -> bronze.bls_ces_raw.

Runs on the 25th (BLS publishes State and Metro Area employment around the
third week). Re-fetches the last `lookback_years` because BLS revises metro
estimates every March (annual benchmark); the MERGE keeps it idempotent.

Candidate series come from the HUD zip->CBSA crosswalk in Bronze.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task

from rpof_assets import BRONZE_BLS_CES


def _alert(context):
    from ingestion.alerts import task_failure_alert

    task_failure_alert(context)


@dag(
    dag_id="ingest_bls_ces",
    schedule="0 7 25 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": _alert,
    },
    params={"lookback_years": Param(2, type="integer", minimum=0, maximum=19)},
    tags=["phase-3", "ingest", "bls"],
)
def ingest_bls_ces():
    @task
    def extract(params=None, data_interval_end=None, dag_run=None) -> list[str]:
        from ingestion.bls_pipeline import extract_to_raw

        year = (data_interval_end or dag_run.run_after).year
        return extract_to_raw(year - params["lookback_years"], year)

    @task(outlets=[BRONZE_BLS_CES])
    def load_bronze(keys: list[str], run_id=None) -> dict:
        from ingestion.bls_pipeline import load_raw_to_bronze

        return load_raw_to_bronze(keys, run_id=run_id)

    load_bronze(extract())


ingest_bls_ces()
