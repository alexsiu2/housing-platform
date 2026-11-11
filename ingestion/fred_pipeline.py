"""
FRED ingestion pipeline: API -> raw zone -> validation -> Bronze Delta.

Used by both the Airflow DAG (dags/ingest_fred.py) and the backfill script
(scripts/backfill_fred.py), so scheduled runs and backfills behave the same.
"""

import json
import logging
from datetime import datetime, timezone

import pandas as pd

from ingestion.bronze_loader import merge_to_delta
from ingestion.fred_client import FRED_SERIES, fetch_series_json, parse_observations
from ingestion.storage import (
    delta_storage_options,
    get_s3_client,
    lake_uri,
    put_raw,
    raw_bucket,
)
from ingestion.validation import validate_fred_observations

logger = logging.getLogger(__name__)

BRONZE_TABLE = "fred_series_raw"
BRONZE_KEYS = ["series_id", "date"]


def raw_key(series_id: str, run_ts: datetime) -> str:
    """Raw zone path, unique per series per run: fred/YYYY/MM/DD/<id>_<time>.json"""
    return f"fred/{run_ts:%Y/%m/%d}/{series_id}_{run_ts:%H%M%S%f}.json"


def extract_to_raw(
    start_date: str,
    end_date: str,
    series_ids: list[str] | None = None,
    run_ts: datetime | None = None,
    client=None,
) -> list[str]:
    """
    Fetch each series and land the untouched API payload in the raw zone,
    wrapped in an envelope recording what was requested and when.

    Returns the raw keys written. One failing series is logged and skipped;
    if every series fails, raises RuntimeError.
    """
    series_ids = series_ids or list(FRED_SERIES)
    run_ts = run_ts or datetime.now(timezone.utc)
    client = client or get_s3_client()

    keys, failed = [], []
    for series_id in series_ids:
        try:
            payload = fetch_series_json(series_id, start_date, end_date)
        except Exception as e:
            logger.error("Extract failed for %s: %s", series_id, e)
            failed.append(series_id)
            continue

        envelope = {
            "source": "fred",
            "series_id": series_id,
            "observation_start": start_date,
            "observation_end": end_date,
            "fetched_at": run_ts.isoformat(),
            "response": payload,
        }
        key = raw_key(series_id, run_ts)
        put_raw(key, json.dumps(envelope).encode(), client=client)
        keys.append(key)

    if not keys:
        raise RuntimeError(f"Failed to extract any FRED series: {failed}")
    if failed:
        logger.warning("Extracted %d series; failed: %s", len(keys), failed)
    return keys


def read_raw(keys: list[str], client=None) -> pd.DataFrame:
    """Parse landed raw envelopes into one DataFrame with source_file lineage."""
    client = client or get_s3_client()
    bucket = raw_bucket()

    frames = []
    for key in keys:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        envelope = json.loads(body)
        df = parse_observations(
            envelope["response"],
            envelope["series_id"],
            ingested_at=datetime.fromisoformat(envelope["fetched_at"]),
        )
        df["source_file"] = f"s3://{bucket}/{key}"
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def load_raw_to_bronze(
    keys: list[str],
    client=None,
    table_uri: str | None = None,
    storage_options: dict | None = None,
) -> dict:
    """Validate landed raw files and merge them into bronze.fred_series_raw."""
    df = read_raw(keys, client=client)
    validation = validate_fred_observations(df, known_series=list(FRED_SERIES))

    metrics = merge_to_delta(
        df,
        table_uri or lake_uri("bronze", BRONZE_TABLE),
        keys=BRONZE_KEYS,
        storage_options=(
            storage_options if storage_options is not None else delta_storage_options()
        ),
        partition_by=["series_id"],
    )
    metrics["validation"] = validation
    metrics["raw_files"] = len(keys)
    return metrics
