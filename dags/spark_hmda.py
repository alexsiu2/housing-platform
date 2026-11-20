"""
HMDA mortgage data through Spark (design doc §8.1 spark_hmda, §8.3).

Per requested activity year:
  download  FFIEC snapshot zip (0.6-1.5 GB) -> raw zone (reused if present)
  stage     zip -> .csv.gz in the lake's staging area (Spark can't read zip)
  hmda_lar  SparkSubmitOperator: jobs/hmda_lar.py on the `spark` profile
            -> bronze.hmda_lar_raw + silver_spark.zip_lending_activity
  clear     remove the staged copy
Then `publish` emits one SILVER_SPARK_LENDING event -> one dbt_build.

Spark runs one year at a time (max_active_tis_per_dagrun=1) to stay inside
the Docker memory budget. The driver runs here in the scheduler (client
mode); executors on spark-worker connect back to it on fixed ports.

Annual run (mid-July, after the FFIEC snapshot for the prior year) loads the
prior year. Trigger manually with `years`, e.g. [2018, ..., 2024], to backfill.
Needs `make up-spark`.
"""

from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Param, dag, task

from rpof_assets import SILVER_SPARK_LENDING

JOB = "/opt/airflow/project/jobs/hmda_lar.py"

SPARK_CONF = {
    # Client mode: executors connect back to the driver in this container.
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.port": "7078",
    "spark.blockManager.port": "7079",
    # Delta + S3A jars are baked into the image (docker/spark/jars.txt);
    # executors have them in /opt/spark/jars, so nothing is shipped.
    "spark.driver.extraClassPath": "/opt/rpof-spark-jars/*",
    "spark.ui.enabled": "false",
}


def _alert(context):
    from ingestion.alerts import task_failure_alert

    task_failure_alert(context)


def _job_args(year: int) -> list[str]:
    return ["--year", str(year), "--run-id", "{{ run_id }}"]


@dag(
    dag_id="spark_hmda",
    schedule="0 6 15 7 *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": _alert,
    },
    params={
        "years": Param(
            None,
            type=["null", "array"],
            items={"type": "integer", "minimum": 2018},
            description="Activity years to load. Empty = the year before the run.",
        )
    },
    tags=["phase-5", "spark", "hmda"],
)
def spark_hmda():
    @task
    def resolve_years(params=None, data_interval_end=None, dag_run=None) -> list[int]:
        if params.get("years"):
            return sorted(set(params["years"]))
        return [(data_interval_end or dag_run.run_after).year - 1]

    @task(max_active_tis_per_dagrun=2)
    def download(year: int) -> int:
        from ingestion.hmda_pipeline import download_to_raw

        download_to_raw(year)
        return year

    @task(max_active_tis_per_dagrun=1)  # temp files on disk: one at a time
    def stage(year: int) -> int:
        from ingestion.hmda_pipeline import stage_for_spark

        stage_for_spark(year)
        return year

    years = resolve_years()
    staged = stage.expand(year=download.expand(year=years))

    spark_jobs = SparkSubmitOperator.partial(
        task_id="hmda_lar",
        application=JOB,
        conn_id="spark_default",
        name="hmda_lar",
        conf=SPARK_CONF,
        driver_memory="768m",
        executor_memory="1800m",
        executor_cores=2,
        total_executor_cores=2,
        max_active_tis_per_dagrun=1,  # one Spark job at a time (memory)
        retries=0,
    ).expand(application_args=staged.map(_job_args))

    @task
    def clear(year: int) -> None:
        from ingestion.hmda_pipeline import clear_staging

        clear_staging(year)

    @task(outlets=[SILVER_SPARK_LENDING])
    def publish(years: list[int]) -> list[int]:
        return years

    cleared = clear.expand(year=years)
    spark_jobs >> cleared
    cleared >> publish(years)


spark_hmda()
