"""
Opportunity scores: runs after every green dbt_build (GOLD_METRICS asset).

Reads gold.gold_zip_investment_metrics for the latest month, applies
config/scoring.yaml, checks the result and replaces gold.zip_opportunity_score.
A failed check raises before anything is written.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task

from rpof_assets import GOLD_METRICS


def _alert(context):
    from ingestion.alerts import task_failure_alert

    task_failure_alert(context)


@dag(
    dag_id="compute_scores",
    schedule=[GOLD_METRICS],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": _alert,
    },
    params={
        "month": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Snapshot month YYYY-MM-01. Empty = latest.",
        )
    },
    tags=["phase-3", "scoring"],
)
def compute_scores():
    @task
    def score(params=None) -> dict:
        from scoring.score import run

        return run(month=params.get("month"))

    score()


compute_scores()
