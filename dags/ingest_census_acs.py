"""
Annual Census ACS 5-year ZCTA data: API -> raw zone -> GE -> census_acs_raw.

Census releases 5-year vintage V in December of V+1, so the January run loads
vintage (year - 2). Trigger manually with `vintage_year` to load another
(e.g. a backfill of older vintages for point-in-time history).
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task

from rpof_assets import BRONZE_CENSUS_ACS


def _alert(context):
    from ingestion.alerts import task_failure_alert

    task_failure_alert(context)


@dag(
    dag_id="ingest_census_acs",
    schedule="0 7 15 1 *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": _alert,
    },
    params={
        "vintage_year": Param(
            None,
            type=["null", "integer"],
            minimum=2011,
            description="ACS 5-year vintage. Empty = run year - 2.",
        )
    },
    tags=["phase-3", "ingest", "census"],
)
def ingest_census_acs():
    @task
    def extract(params=None, data_interval_end=None, dag_run=None) -> str:
        from ingestion.census_pipeline import extract_to_raw

        year = (data_interval_end or dag_run.run_after).year
        return extract_to_raw(params.get("vintage_year") or year - 2)

    @task(outlets=[BRONZE_CENSUS_ACS])
    def load_bronze(key: str, run_id=None) -> dict:
        from ingestion.census_pipeline import load_raw_to_bronze

        return load_raw_to_bronze(key, run_id=run_id)

    load_bronze(extract())


ingest_census_acs()
