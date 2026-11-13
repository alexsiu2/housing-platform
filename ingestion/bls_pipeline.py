"""
BLS CES metro employment: API -> raw zone -> validation -> bronze.bls_ces_raw.

Candidate series come from the HUD zip->CBSA crosswalk in Bronze: one per
(metro, state) pair, so multi-state metros are tried under each state. dbt
(silver_metro_job_growth) keeps one series per metro. Keyed on
(series_id, month); re-runs and overlapping windows are idempotent.
"""

import json
import logging
from datetime import datetime, timezone

import pandas as pd
from deltalake import DeltaTable

from ingestion.bls_client import ces_series_id, fetch_series, parse_payload
from ingestion.bronze_loader import merge_to_delta
from ingestion.storage import (
    delta_storage_options,
    get_s3_client,
    lake_uri,
    put_raw,
    raw_bucket,
)
from ingestion.validation import validate_bls_ces

logger = logging.getLogger(__name__)

BRONZE_TABLE = "bls_ces_raw"
BRONZE_KEYS = ["series_id", "month"]

# USPS state abbreviation -> state FIPS (50 states, DC, PR).
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56", "PR": "72",
}  # fmt: skip


def candidate_series(crosswalk: pd.DataFrame) -> list[str]:
    """
    CES series IDs to try, from zip-cbsa crosswalk rows (latest vintage).

    One candidate per (CBSA, state) pair; HUD's 99999 ("no CBSA") is skipped.
    """
    df = crosswalk[
        (crosswalk["crosswalk_type"] == "zip-cbsa") & (crosswalk["geoid"] != "99999")
    ]
    df = df[df["vintage"] == df["vintage"].max()]
    pairs = df[["geoid", "state"]].dropna().drop_duplicates()
    unknown = sorted(set(pairs["state"]) - set(STATE_FIPS))
    if unknown:
        logger.warning("No FIPS code for states %s; skipping them", unknown)
    pairs = pairs[pairs["state"].isin(STATE_FIPS)]
    return sorted(ces_series_id(STATE_FIPS[s], g) for g, s in pairs.itertuples(False))


def read_crosswalk(table_uri: str | None = None, storage_options: dict | None = None):
    dt = DeltaTable(
        table_uri or lake_uri("bronze", "hud_crosswalk_raw"),
        storage_options=(
            storage_options if storage_options is not None else delta_storage_options()
        ),
    )
    return dt.to_pandas(
        columns=["crosswalk_type", "vintage", "geoid", "state"],
        filters=[("crosswalk_type", "=", "zip-cbsa")],
    )


def raw_key(run_ts: datetime, batch: int) -> str:
    return f"bls/ces/{run_ts:%Y/%m}/batch_{batch:03d}_{run_ts:%d%H%M%S%f}.json"


def extract_to_raw(
    start_year: int,
    end_year: int | None = None,
    series_ids: list[str] | None = None,
    run_ts: datetime | None = None,
    client=None,
) -> list[str]:
    """Fetch every candidate series and land each API response untouched."""
    run_ts = run_ts or datetime.now(timezone.utc)
    series_ids = series_ids or candidate_series(read_crosswalk())
    payloads = fetch_series(series_ids, start_year, end_year)
    keys = []
    for i, payload in enumerate(payloads):
        envelope = {
            "source": "bls_ces",
            "fetched_at": run_ts.isoformat(),
            "start_year": start_year,
            "end_year": end_year,
            "response": payload,
        }
        key = raw_key(run_ts, i)
        put_raw(key, json.dumps(envelope).encode(), client=client)
        keys.append(key)
    logger.info("Landed %d BLS responses for %d series", len(keys), len(series_ids))
    return keys


def load_raw_to_bronze(
    keys: list[str],
    client=None,
    table_uri: str | None = None,
    storage_options: dict | None = None,
    run_id: str | None = None,
) -> dict:
    """Parse landed responses, validate, and MERGE into bronze.bls_ces_raw."""
    client = client or get_s3_client()
    bucket = raw_bucket()
    frames = []
    for key in keys:
        envelope = json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
        df = parse_payload(envelope["response"])
        df["source_file"] = f"s3://{bucket}/{key}"
        df["ingested_at"] = datetime.fromisoformat(envelope["fetched_at"])
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        raise RuntimeError("BLS returned no data for any candidate series")

    validation = validate_bls_ces(df)
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
            "validation": validation,
            "series_with_data": int(df["series_id"].nunique()),
            "areas": int(df["area_code"].nunique()),
            "min_month": df["month"].min().date().isoformat(),
            "max_month": df["month"].max().date().isoformat(),
        }
    )
    return metrics
