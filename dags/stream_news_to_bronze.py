"""
Kafka news topics -> bronze.news_events_raw every 15 minutes (design §9.1, D16).

  kafka_up   skip the run (not fail) when the `streaming` profile is down
  stream     jobs/stream_news_to_bronze.py: Structured Streaming with
             trigger(availableNow), Spark in local mode inside this task.
             It drains new events since the checkpoint and exits.

News volume is a few hundred events an hour, so a Spark cluster would sit
idle: local[2] in the task uses ~1 GB for about a minute, then nothing.

The Bronze asset event is lineage only. It is deliberately not a dbt
trigger: rebuilding every 15 minutes would hold the warehouse lock and
rescore constantly. dbt_build refreshes the news models daily, and
llm_metro_outlook rebuilds them before it runs.
"""

import socket
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag, task

from rpof_assets import BRONZE_NEWS

JOB = "/opt/airflow/project/jobs/stream_news_to_bronze.py"

SPARK_CONF = {
    # Local mode: driver and executor are this task's JVM. Kafka, Avro, Delta
    # and S3A jars are baked into the image (docker/spark/jars.txt).
    "spark.driver.extraClassPath": "/opt/rpof-spark-jars/*",
    "spark.ui.enabled": "false",
    "spark.sql.session.timeZone": "UTC",
}


def _alert(context):
    from ingestion.alerts import task_failure_alert

    task_failure_alert(context)


@dag(
    dag_id="stream_news_to_bronze",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=14),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": _alert,
    },
    tags=["phase-6", "streaming", "spark", "kafka"],
)
def stream_news_to_bronze():
    @task.short_circuit
    def kafka_up() -> bool:
        import os

        host, _, port = os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
        ).partition(":")
        try:
            with socket.create_connection((host, int(port or 9092)), timeout=5):
                return True
        except OSError:
            print("Kafka is not reachable; start it with `make up-streaming`.")
            return False

    stream = SparkSubmitOperator(
        task_id="stream",
        application=JOB,
        conn_id="spark_local",
        name="stream_news_to_bronze",
        conf=SPARK_CONF,
        driver_memory="1g",
        application_args=["--run-id", "{{ run_id }}"],
        outlets=[BRONZE_NEWS],
    )

    kafka_up() >> stream


stream_news_to_bronze()
