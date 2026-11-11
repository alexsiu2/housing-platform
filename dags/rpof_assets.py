"""
Airflow Assets for the lake, shared by producer and consumer DAGs.

An ingest DAG lists a Bronze asset in a task's `outlets`; dbt_build is
scheduled on ANY_BRONZE_UPDATE, so Silver rebuilds whenever any Bronze table
changes (design doc §8.1, data-aware scheduling).

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

BRONZE_ASSETS = [BRONZE_FRED, BRONZE_ZORI, BRONZE_ZHVI, BRONZE_HUD_CROSSWALK]

# Triggers when any one Bronze table updates.
ANY_BRONZE_UPDATE = reduce(or_, BRONZE_ASSETS)
