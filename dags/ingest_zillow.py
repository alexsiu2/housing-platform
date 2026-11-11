"""
Monthly Zillow ingest: ZORI (rents) + ZHVI (home values) by zip.

Per dataset:  extract (download -> raw zone)  ->  load_bronze (melt -> GE -> MERGE)

Runs on the 20th: Zillow refreshes these files mid-month (last seen on the
16th). A run merges the trailing `lookback_months` because Zillow's smoothing
revises recent months; set lookback_months=0 on a manual run to reload the
full history (or use `make backfill-zillow`).

Each successful load updates its Bronze asset, which schedules dbt_build.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task

from rpof_assets import BRONZE_ZHVI, BRONZE_ZORI

OUTLETS = {"zori": BRONZE_ZORI, "zhvi": BRONZE_ZHVI}


@dag(
    dag_id="ingest_zillow",
    schedule="0 6 20 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    params={
        "lookback_months": Param(
            24,
            type="integer",
            minimum=0,
            description="Months to re-merge. 0 = full history.",
        ),
    },
    tags=["phase-2", "ingest", "zillow"],
)
def ingest_zillow():
    for dataset, asset in OUTLETS.items():

        @task(task_id=f"extract_{dataset}")
        def extract(ds_name: str = dataset) -> str:
            from ingestion.zillow_pipeline import extract_to_raw

            return extract_to_raw(ds_name)

        @task(task_id=f"load_bronze_{dataset}", outlets=[asset])
        def load_bronze(
            key: str,
            ds_name: str = dataset,
            params=None,
            run_id=None,
            data_interval_end=None,
            dag_run=None,
        ) -> dict:
            from ingestion.zillow_pipeline import load_raw_to_bronze

            anchor = data_interval_end or dag_run.run_after
            return load_raw_to_bronze(
                ds_name,
                key,
                lookback_months=params["lookback_months"] or None,
                today=anchor.date(),
                run_id=run_id,
            )

        load_bronze(extract())


ingest_zillow()
