"""
Quarterly HUD USPS crosswalks: zip-cbsa and tract-zip.

Per crosswalk:  extract (API -> raw zone)  ->  load_bronze (GE -> MERGE)

HUD publishes each quarter's files with a lag, so this runs mid-quarter and
asks for the latest vintage; a vintage already loaded is merged idempotently.
Set year/quarter on a manual run to load a specific vintage.

Once both loads succeed, `publish` updates the Bronze asset once, so one run
schedules exactly one dbt_build.

Needs HUD_API_TOKEN in .env.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task

from rpof_assets import BRONZE_HUD_CROSSWALK

CROSSWALKS = ["zip-cbsa", "tract-zip"]


def _alert(context):
    from ingestion.alerts import task_failure_alert

    task_failure_alert(context)


@dag(
    dag_id="ingest_hud_crosswalk",
    schedule="0 7 15 2,5,8,11 *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": _alert,
    },
    params={
        "year": Param(None, type=["null", "integer"], minimum=2010),
        "quarter": Param(None, type=["null", "integer"], minimum=1, maximum=4),
    },
    tags=["phase-2", "ingest", "hud"],
)
def ingest_hud_crosswalk():
    @task(outlets=[BRONZE_HUD_CROSSWALK])
    def publish(results: list[dict]) -> list[str]:
        return [f"{r['crosswalk']} {r['vintage']}" for r in results]

    loads = []
    for crosswalk in CROSSWALKS:
        name = crosswalk.replace("-", "_")

        @task(task_id=f"extract_{name}")
        def extract(xwalk: str = crosswalk, params=None) -> str:
            from ingestion.hud_pipeline import extract_to_raw

            return extract_to_raw(xwalk, params.get("year"), params.get("quarter"))

        @task(task_id=f"load_bronze_{name}")
        def load_bronze(key: str, run_id=None) -> dict:
            from ingestion.hud_pipeline import load_raw_to_bronze

            return load_raw_to_bronze(key, run_id=run_id)

        loads.append(load_bronze(extract()))

    publish(loads)


ingest_hud_crosswalk()
