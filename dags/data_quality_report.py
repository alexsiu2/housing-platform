"""
Daily data quality report (design doc §8.1, §10.1).

Checks every Bronze table's freshness against its expected cadence and
alerts if any is missing or overdue (> 2x cadence). Ingest DAGs alert on
their own failures; this catches the silent kind: a DAG paused or never run.
"""

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="data_quality_report",
    schedule="0 8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["phase-3", "quality"],
)
def data_quality_report():
    @task
    def freshness() -> list[dict]:
        from ingestion.alerts import send_alert
        from ingestion.quality_report import freshness_report, problems

        report = freshness_report()
        issues = problems(report)
        if issues:
            send_alert("[rpof] data quality: " + "; ".join(issues))
            raise RuntimeError("Stale or missing Bronze tables: " + "; ".join(issues))
        return report

    freshness()


data_quality_report()
