"""
Phase 0 smoke test: proves Airflow can reach object storage and write Delta.

Trigger manually from the Airflow UI after `make up`. It lands a small file in
the raw bucket and merges two rows into a throwaway Delta table in the lake.
"""

from datetime import datetime, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="platform_smoke_test",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["phase-0", "smoke"],
)
def platform_smoke_test():
    @task
    def land_raw_file() -> str:
        from ingestion.storage import put_raw

        now = datetime.now(timezone.utc)
        key = f"_smoke/{now:%Y/%m/%d}/{now:%H%M%S%f}.txt"
        return put_raw(key, b"hello from airflow")

    @task
    def write_delta(raw_uri: str) -> dict:
        import pandas as pd

        from ingestion.bronze_loader import merge_to_delta
        from ingestion.storage import delta_storage_options, lake_uri

        df = pd.DataFrame(
            {
                "check": ["raw_landed", "delta_written"],
                "detail": [raw_uri, "ok"],
                "checked_at": [datetime.now(timezone.utc)] * 2,
            }
        )
        return merge_to_delta(
            df,
            lake_uri("_smoke", "platform_check"),
            keys=["check"],
            storage_options=delta_storage_options(),
        )

    write_delta(land_raw_file())


platform_smoke_test()
