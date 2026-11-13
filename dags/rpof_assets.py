"""
Airflow Assets for the lake, shared by producer and consumer DAGs.

Load tasks list their Bronze table asset in `outlets` (lineage in the UI).
dbt_build is scheduled on ANY_BRONZE_UPDATE, one event per successful ingest
run, so Silver rebuilds whenever Bronze changes (design doc §8.1).

Note: a plain list as a schedule means "all of these updated"; `|` means any.

Kept free of heavy imports so DAG parsing stays fast.
"""

import os
from functools import reduce
from operator import or_

from airflow.sdk import Asset

_LAKE = os.getenv("LAKE_BUCKET", "rpof-lake")


def _bronze(table: str) -> Asset:
    return Asset(name=f"bronze.{table}", uri=f"s3://{_LAKE}/bronze/{table}")


BRONZE_FRED = _bronze("fred_series_raw")
BRONZE_ZORI = _bronze("zori_raw")
BRONZE_ZHVI = _bronze("zhvi_raw")
BRONZE_HUD_CROSSWALK = _bronze("hud_crosswalk_raw")
BRONZE_BLS_CES = _bronze("bls_ces_raw")
BRONZE_CENSUS_ACS = _bronze("census_acs_raw")

# Emitted by dbt_build after a green build; schedules compute_scores.
GOLD_METRICS = Asset(
    name="gold.gold_zip_investment_metrics",
    uri="duckdb://rpof/gold/gold_zip_investment_metrics",
)

# One "ready" signal per multi-table ingest DAG. Airflow creates a separate
# asset-triggered run per event, so if dbt_build listened to zori_raw and
# zhvi_raw directly, one Zillow run would queue two dbt builds.
ZILLOW_READY = Asset(
    name="bronze.zillow_ready", uri=f"s3://{_LAKE}/bronze/_ready/zillow"
)

# What dbt_build listens to: exactly one event per successful ingest run.
DBT_TRIGGERS = [
    BRONZE_FRED,
    ZILLOW_READY,
    BRONZE_HUD_CROSSWALK,
    BRONZE_BLS_CES,
    BRONZE_CENSUS_ACS,
]

# Triggers when any one of them updates.
ANY_BRONZE_UPDATE = reduce(or_, DBT_TRIGGERS)
