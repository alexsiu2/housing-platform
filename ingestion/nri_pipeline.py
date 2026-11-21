"""
FEMA NRI: feature service -> raw zone -> validation -> bronze.fema_nri_raw.

Keyed on (tract, nri_release): a new FEMA release adds rows instead of
overwriting, and re-running the same release is idempotent. dbt uses the
newest release.
"""

import json
import logging
from datetime import datetime, timezone

from ingestion.bronze_loader import merge_to_delta
from ingestion.nri_client import fetch_all_pages, parse_tracts, release_date
from ingestion.storage import (
    delta_storage_options,
    get_s3_client,
    lake_uri,
    put_raw,
    raw_bucket,
)
from ingestion.validation import validate_fema_nri

logger = logging.getLogger(__name__)

BRONZE_TABLE = "fema_nri_raw"
BRONZE_KEYS = ["tract", "nri_release"]


def raw_key(release: str, run_ts: datetime) -> str:
    return f"fema/nri/{release}/tracts_{run_ts:%Y%m%dT%H%M%S%f}.json"


def extract_to_raw(run_ts: datetime | None = None, client=None) -> str:
    run_ts = run_ts or datetime.now(timezone.utc)
    release = release_date()
    rows = fetch_all_pages()
    envelope = {
        "source": "fema_nri_arcgis",
        "nri_release": release,
        "fetched_at": run_ts.isoformat(),
        "rows": rows,
    }
    key = raw_key(release, run_ts)
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
    df = parse_tracts(envelope["rows"], envelope["nri_release"])
    df["source_file"] = f"s3://{bucket}/{key}"
    df["ingested_at"] = datetime.fromisoformat(envelope["fetched_at"])
    validation = validate_fema_nri(df)
    metrics = merge_to_delta(
        df,
        table_uri or lake_uri("bronze", BRONZE_TABLE),
        keys=BRONZE_KEYS,
        storage_options=(
            storage_options if storage_options is not None else delta_storage_options()
        ),
        run_id=run_id,
    )
    metrics.update(
        {
            "nri_release": envelope["nri_release"],
            "validation": validation,
            "tracts": int(df["tract"].nunique()),
        }
    )
    return metrics
