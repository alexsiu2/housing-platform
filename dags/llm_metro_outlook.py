"""
Weekly metro news outlook with Claude (design doc §9.4, D17).

Mondays 10:00 UTC, for the last complete Monday-Sunday week (after the
Monday GDELT ingest, so the tone baseline is current):

  build_news  dbt: rebuild the streamed news models (silver_news_items)
  prepare     pick metros, gather the week's items, land the request
              manifest in the raw zone (llm/metro_outlook/...)
  submit      one Message Batch (Batch API, half price)
  wait        sensor, rescheduled every 5 minutes (a batch can take up to 24 h;
              it holds no worker slot in between)
  collect     check results (stop reason, schema, citations), retry once
              directly what may succeed, land the results, write Gold
  export      refresh the dashboard extract

Model and limits: config/llm.yaml. Needs ANTHROPIC_API_KEY in .env.
Trigger manually with week_start (a Monday) to redo or backfill a week; the
week's rows are replaced, other weeks are untouched.
"""

from datetime import date, datetime, timedelta

from airflow.sdk import Param, dag, task

from rpof_assets import GOLD_NEWS_OUTLOOK

DBT_BIN = "/opt/dbt-venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/project/dbt"
NEWS_SELECT = "source:bronze.news_events_raw+"


def _alert(context):
    from ingestion.alerts import task_failure_alert

    task_failure_alert(context)


@dag(
    dag_id="llm_metro_outlook",
    schedule="0 10 * * 1",  # Mondays 10:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": _alert,
    },
    params={
        "week_start": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Monday of the week to summarize. Empty = last full week.",
        ),
    },
    tags=["phase-6", "llm", "news"],
)
def llm_metro_outlook():
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
    def build_news() -> str:
        return (
            "python -m ingestion.warehouse_lock run -- "
            f"{DBT_BIN} build --select {NEWS_SELECT}"
        )

    @task
    def prepare(params=None, dag_run=None) -> str | None:
        """Returns the manifest's raw-zone key, or None if there is nothing to do."""
        from ingestion.warehouse_lock import warehouse_lock
        from llm.summarize import land_json, last_complete_week, load_config
        from llm.summarize import manifest_key
        from llm.summarize import prepare as build

        if params.get("week_start"):
            week = date.fromisoformat(params["week_start"])
            if week.weekday() != 0:
                raise ValueError(f"week_start {week} is not a Monday")
        else:
            week = last_complete_week(dag_run.run_after.date())
        with warehouse_lock("llm_prepare"):
            manifest = build(week, load_config())
        print(f"Week {week}: {len(manifest['entries'])} outlooks to request; "
              f"too little news: {manifest['skipped_low_news']}")  # fmt: skip
        if not manifest["entries"]:
            return None
        return land_json(manifest_key(manifest), manifest)

    @task
    def submit(key: str | None) -> str | None:
        from llm.summarize import anthropic_client, read_raw_json, submit_batch

        if key is None:
            return None
        return submit_batch(anthropic_client(), read_raw_json(key))

    @task.sensor(poke_interval=300, timeout=26 * 3600, mode="reschedule")
    def wait(batch_id: str | None) -> bool:
        from llm.summarize import anthropic_client, batch_ended

        return batch_id is None or batch_ended(anthropic_client(), batch_id)

    @task(outlets=[GOLD_NEWS_OUTLOOK])
    def collect(key: str | None, batch_id: str | None, _ready=None) -> dict:
        from ingestion.warehouse_lock import warehouse_lock
        from llm import summarize as s

        if key is None:
            return {"written": 0}
        cfg = s.load_config()
        manifest = s.read_raw_json(key)
        outcomes = s.collect(s.anthropic_client(), manifest, batch_id, cfg)
        s.land_json(s.results_key(key), {"batch_id": batch_id, "outcomes": outcomes})
        with warehouse_lock("llm_store"):
            summary = s.store(manifest, outcomes, batch_id)
        print(summary)
        if manifest["entries"] and summary["written"] == 0:
            raise RuntimeError(f"No outlook passed the checks: {summary['rejected']}")
        return summary

    @task
    def export_dashboard(summary: dict) -> dict:
        from ingestion.warehouse_lock import warehouse_lock
        from serving.dashboard_export import export

        with warehouse_lock("export_dashboard"):
            return export()

    key = prepare()
    batch_id = submit(key)
    ready = wait(batch_id)
    build_news() >> key
    export_dashboard(collect(key, batch_id, ready))


llm_metro_outlook()
