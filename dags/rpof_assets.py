"""
Airflow Assets for the lake, shared by producer and consumer DAGs.

An ingest DAG lists a Bronze asset in a task's `outlets`; dbt_build is
scheduled on those assets, so Silver rebuilds whenever Bronze changes
(design doc §8.1, data-aware scheduling).

Kept free of heavy imports so DAG parsing stays fast.
"""

import os

from airflow.sdk import Asset

_LAKE = os.getenv("LAKE_BUCKET", "rpof-lake")

BRONZE_FRED = Asset(
    name="bronze.fred_series_raw",
    uri=f"s3://{_LAKE}/bronze/fred_series_raw",
)

# Every Bronze asset dbt should react to. Later phases append Zillow, Census...
BRONZE_ASSETS = [BRONZE_FRED]
