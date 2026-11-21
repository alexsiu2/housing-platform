"""
Build and test dbt models whenever a Bronze table updates.

Scheduled on any Bronze asset update (rpof_assets.ANY_BRONZE_UPDATE): a
successful ingest_fred, ingest_zillow or ingest_hud_crosswalk run rebuilds
everything downstream of Bronze and runs its tests. Any failing test (not
warning) fails the task.

dbt lives in its own virtualenv in the image (/opt/dbt-venv) so its pins
never fight Airflow's. Artifacts and logs go to /tmp, not the mounted repo.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task

from rpof_assets import ANY_BRONZE_UPDATE, GOLD_METRICS

DBT_BIN = "/opt/dbt-venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/project/dbt"


def _alert(context):
    from ingestion.alerts import task_failure_alert

    task_failure_alert(context)


@dag(
    dag_id="dbt_build",
    schedule=ANY_BRONZE_UPDATE,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": _alert,
    },
    params={
        # Restricted to selector characters so the value is safe in a shell.
        "select": Param(
            # Manual runs only. Every source layer: Bronze (ingestion) and
            # silver_spark (Spark jobs); asset-triggered runs build selectively.
            "source:bronze+ source:silver_spark+",
            type="string",
            pattern=r"^[A-Za-z0-9_.:+*@,/ -]+$",
            description="dbt node selection. Default: everything downstream "
            "of all sources.",
        ),
    },
    tags=["phase-1", "dbt"],
)
def dbt_build():
    @task.bash(
        outlets=[GOLD_METRICS],  # a green build schedules compute_scores
        cwd=DBT_PROJECT_DIR,
        env={
            "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
            "DBT_TARGET_PATH": "/tmp/dbt/target",
            "DBT_LOG_PATH": "/tmp/dbt/logs",
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        },
        append_env=True,
    )
    def build(params=None, triggering_asset_events=None, dag_run=None) -> str:
        from dbt_selection import FULL_BUILD, selector_for

        # Asset-triggered: build only downstream of what changed (a full
        # rebuild reloads every Bronze source and was OOM-killed). Manual
        # runs use the `select` param (default: everything).
        triggered = [a.name for a in (triggering_asset_events or {})]
        select = selector_for(triggered) if triggered else params["select"]
        if select != FULL_BUILD:
            print(f"Selective build for {triggered}: {select}")
        # One writer at a time: DuckDB's own file lock does not work across
        # Docker Desktop's file sharing (see ingestion/warehouse_lock.py).
        return (
            "python -m ingestion.warehouse_lock run -- "
            f"{DBT_BIN} build --select {select}"
        )

    build()


dbt_build()
