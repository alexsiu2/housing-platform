"""
Zillow ingestion pipeline: download -> raw zone -> melt -> validation -> Bronze.

Bronze tables: bronze.zori_raw, bronze.zhvi_raw, keyed on (zip, month) and
partitioned by year. Shared by dags/ingest_zillow.py and
scripts/backfill_zillow.py.

Monthly runs merge only the trailing `lookback_months` (Zillow's smoothing
revises recent months); a backfill merges the full history.
"""

import logging
from datetime import date, datetime, timezone

import pandas as pd

from ingestion.bronze_loader import merge_to_delta
from ingestion.storage import (
    delta_storage_options,
    get_s3_client,
    lake_uri,
    put_raw,
    raw_bucket,
)
from ingestion.validation import validate_zillow
from ingestion.zillow_client import DATASETS, download_csv, parse_wide_csv

logger = logging.getLogger(__name__)

BRONZE_KEYS = ["zip", "month"]
DEFAULT_LOOKBACK_MONTHS = 24


def bronze_table(dataset: str) -> str:
    return f"{dataset}_raw"


def raw_key(dataset: str, run_ts: datetime) -> str:
    """zillow/<dataset>/YYYY/MM/<file stem>_<time>.csv (unique per run)."""
    stem = DATASETS[dataset][0].rsplit("/", 1)[-1].removesuffix(".csv")
    return f"zillow/{dataset}/{run_ts:%Y/%m}/{stem}_{run_ts:%d%H%M%S%f}.csv"


def since_month(lookback_months: int | None, today: date | None = None) -> str | None:
    """First month of the trailing window, or None for full history."""
    if not lookback_months:
        return None
    start = pd.Timestamp(today or date.today()).to_period("M") - lookback_months
    return start.to_timestamp().date().isoformat()


def extract_to_raw(dataset: str, run_ts: datetime | None = None, client=None) -> str:
    """Download one dataset and land the untouched CSV. Returns the raw key."""
    run_ts = run_ts or datetime.now(timezone.utc)
    key = raw_key(dataset, run_ts)
    put_raw(key, download_csv(dataset), client=client)
    return key


def load_raw_to_bronze(
    dataset: str,
    key: str,
    lookback_months: int | None = DEFAULT_LOOKBACK_MONTHS,
    today: date | None = None,
    client=None,
    table_uri: str | None = None,
    storage_options: dict | None = None,
    run_id: str | None = None,
) -> dict:
    """
    Parse a landed CSV, validate it, and MERGE it into bronze.<dataset>_raw.

    lookback_months=None loads the full history (backfill).
    """
    client = client or get_s3_client()
    bucket = raw_bucket()
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()

    cutoff = since_month(lookback_months, today)
    df = parse_wide_csv(body, dataset, since_month=cutoff)
    df["source_file"] = f"s3://{bucket}/{key}"
    df["ingested_at"] = datetime.now(timezone.utc)

    value_column = DATASETS[dataset][1]
    validation = validate_zillow(df, dataset, value_column)

    target_filter = f"t.year >= {pd.Timestamp(cutoff).year}" if cutoff else None
    metrics = merge_to_delta(
        df,
        table_uri or lake_uri("bronze", bronze_table(dataset)),
        keys=BRONZE_KEYS,
        storage_options=(
            storage_options if storage_options is not None else delta_storage_options()
        ),
        partition_by=["year"],
        run_id=run_id,
        target_filter=target_filter,
    )
    metrics.update(
        {
            "dataset": dataset,
            "validation": validation,
            "since_month": cutoff,
            "zips": int(df["zip"].nunique()),
            "min_month": df["month"].min().date().isoformat(),
            "max_month": df["month"].max().date().isoformat(),
        }
    )
    return metrics
