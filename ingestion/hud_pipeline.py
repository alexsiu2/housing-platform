"""
HUD crosswalk pipeline: API -> raw zone -> validation -> bronze.hud_crosswalk_raw.

One Bronze table holds every crosswalk and vintage, keyed on
(crosswalk_type, vintage, zip, geoid), so quarterly loads add a new vintage
and re-running a quarter is idempotent. dbt picks the latest vintage.
"""

import json
import logging
from datetime import datetime, timezone

from ingestion.bronze_loader import merge_to_delta
from ingestion.hud_client import CROSSWALKS, fetch_crosswalk_json, parse_crosswalk
from ingestion.storage import (
    delta_storage_options,
    get_s3_client,
    lake_uri,
    put_raw,
    raw_bucket,
)
from ingestion.validation import validate_hud_crosswalk

logger = logging.getLogger(__name__)

BRONZE_TABLE = "hud_crosswalk_raw"
BRONZE_KEYS = ["crosswalk_type", "vintage", "zip", "geoid"]


def raw_key(crosswalk: str, payload: dict, run_ts: datetime) -> str:
    """hud/crosswalk/YYYY_QN/<crosswalk>_<time>.json (vintage from the response)."""
    data = payload.get("data", payload)
    vintage = f"{data['year']}_Q{data['quarter']}"
    return f"hud/crosswalk/{vintage}/{crosswalk}_{run_ts:%Y%m%dT%H%M%S%f}.json"


def extract_to_raw(
    crosswalk: str,
    year: int | None = None,
    quarter: int | None = None,
    run_ts: datetime | None = None,
    client=None,
) -> str:
    """Fetch one crosswalk and land the untouched JSON. Returns the raw key."""
    if crosswalk not in CROSSWALKS:
        raise ValueError(f"Unknown crosswalk '{crosswalk}'")
    run_ts = run_ts or datetime.now(timezone.utc)
    payload = fetch_crosswalk_json(crosswalk, year, quarter)
    key = raw_key(crosswalk, payload, run_ts)
    envelope = {
        "source": "hud_usps_crosswalk",
        "crosswalk": crosswalk,
        "requested": {"year": year, "quarter": quarter},
        "fetched_at": run_ts.isoformat(),
        "response": payload,
    }
    put_raw(key, json.dumps(envelope).encode(), client=client)
    return key


def load_raw_to_bronze(
    key: str,
    client=None,
    table_uri: str | None = None,
    storage_options: dict | None = None,
    run_id: str | None = None,
) -> dict:
    """Parse a landed crosswalk, validate it and MERGE into Bronze."""
    client = client or get_s3_client()
    bucket = raw_bucket()
    envelope = json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
    crosswalk = envelope["crosswalk"]

    df = parse_crosswalk(envelope["response"], crosswalk)
    df["source_file"] = f"s3://{bucket}/{key}"
    df["ingested_at"] = datetime.fromisoformat(envelope["fetched_at"])
    validation = validate_hud_crosswalk(df, crosswalk)

    metrics = merge_to_delta(
        df,
        table_uri or lake_uri("bronze", BRONZE_TABLE),
        keys=BRONZE_KEYS,
        storage_options=(
            storage_options if storage_options is not None else delta_storage_options()
        ),
        partition_by=["crosswalk_type"],
        run_id=run_id,
    )
    metrics.update(
        {
            "crosswalk": crosswalk,
            "vintage": str(df["vintage"].iloc[0]),
            "validation": validation,
            "zips": int(df["zip"].nunique()),
            "geoids": int(df["geoid"].nunique()),
        }
    )
    return metrics
