"""
Census ACS pipeline: API -> raw zone -> validation -> bronze.census_acs_raw.

Keyed on (zcta, variable, vintage_year) and partitioned by vintage, so each
annual release adds a partition and re-loading a vintage is idempotent.
"""

import json
import logging
from datetime import datetime, timezone

from ingestion.bronze_loader import merge_to_delta
from ingestion.census_client import fetch_acs_json, parse_acs
from ingestion.storage import (
    delta_storage_options,
    get_s3_client,
    lake_uri,
    put_raw,
    raw_bucket,
)
from ingestion.validation import validate_census_acs

logger = logging.getLogger(__name__)

BRONZE_TABLE = "census_acs_raw"
BRONZE_KEYS = ["zcta", "variable", "vintage_year"]


def raw_key(vintage_year: int, run_ts: datetime) -> str:
    return f"census/acs5/{vintage_year}/zcta_{run_ts:%Y%m%dT%H%M%S%f}.json"


def extract_to_raw(
    vintage_year: int, run_ts: datetime | None = None, client=None
) -> str:
    run_ts = run_ts or datetime.now(timezone.utc)
    rows = fetch_acs_json(vintage_year)
    envelope = {
        "source": "census_acs5",
        "vintage_year": vintage_year,
        "fetched_at": run_ts.isoformat(),
        "response": rows,
    }
    key = raw_key(vintage_year, run_ts)
    put_raw(key, json.dumps(envelope).encode(), client=client)
    return key


def load_raw_to_bronze(
    key: str,
    client=None,
    table_uri: str | None = None,
    storage_options: dict | None = None,
    run_id: str | None = None,
) -> dict:
    client = client or get_s3_client()
    bucket = raw_bucket()
    envelope = json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
    vintage = int(envelope["vintage_year"])

    df = parse_acs(envelope["response"], vintage)
    df["source_file"] = f"s3://{bucket}/{key}"
    df["ingested_at"] = datetime.fromisoformat(envelope["fetched_at"])
    validation = validate_census_acs(df)

    metrics = merge_to_delta(
        df,
        table_uri or lake_uri("bronze", BRONZE_TABLE),
        keys=BRONZE_KEYS,
        storage_options=(
            storage_options if storage_options is not None else delta_storage_options()
        ),
        partition_by=["vintage_year"],
        run_id=run_id,
    )
    metrics.update(
        {
            "vintage_year": vintage,
            "validation": validation,
            "zctas": int(df["zcta"].nunique()),
            "missing_share": round(float(df["is_missing"].mean()), 4),
        }
    )
    return metrics
