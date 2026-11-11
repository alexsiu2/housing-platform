"""
Build and test dbt models whenever a Bronze table updates.

Scheduled on the Bronze assets (rpof_assets.BRONZE_ASSETS), so a successful
ingest_fred run rebuilds stg_fred__observations -> silver_macro_indicators
and runs their tests. Any failing test fails the task.

dbt lives in its own virtualenv in the image (/opt/dbt-venv) so its pins
never fight Airflow's. Artifacts and logs go to /tmp, not the mounted repo.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task

from rpof_assets import BRONZE_ASSETS

DBT_BIN = "/opt/dbt-venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/project/dbt"


@dag(
    dag_id="dbt_build",
    schedule=BRONZE_ASSETS,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params={
        # Restricted to selector characters so the value is safe in a shell.
        "select": Param(
            "source:bronze+",
            type="string",
            pattern=r"^[A-Za-z0-9_.:+*@,/ -]+$",
            description="dbt node selection. Default: everything downstream of Bronze.",
        ),
    },
    tags=["phase-1", "dbt"],
)
def dbt_build():
    @task.bash(
        cwd=DBT_PROJECT_DIR,
        env={
            "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
            "DBT_TARGET_PATH": "/tmp/dbt/target",
            "DBT_LOG_PATH": "/tmp/dbt/logs",
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        },
        append_env=True,
    )
    def build(params=None) -> str:
        return f"{DBT_BIN} build --select '{params['select']}'"

    build()


dbt_build()
