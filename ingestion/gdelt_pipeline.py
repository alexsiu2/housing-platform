"""
GDELT housing news: BigQuery -> raw zone -> GE -> bronze.gdelt_housing_articles_raw.

Windows are aligned to Mondays and at most `window_days` long, so an ISO week
is never split across two queries (dbt aggregates by week). Each window lands
as one raw JSON file with the BigQuery job stats (job id, GB billed) for cost
auditing, then MERGEs into Bronze on (article_id, url); re-running is idempotent.
"""

import json
import logging
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from ingestion.bronze_loader import merge_to_delta
from ingestion.gdelt_client import DEFAULT_MAX_GB_PER_QUERY, fetch_articles
from ingestion.storage import (
    delta_storage_options,
    get_s3_client,
    lake_uri,
    put_raw,
    raw_bucket,
)
from ingestion.validation import validate_gdelt_articles

logger = logging.getLogger(__name__)

BRONZE_TABLE = "gdelt_housing_articles_raw"
# GDELT occasionally reuses a GKGRECORDID for two different articles (seen in
# translated-feed records, e.g. 20240228201500-T161), so identity is (id, url).
BRONZE_KEYS = ["article_id", "url"]
DEFAULT_WINDOW_DAYS = 28


def monday_on_or_before(d: date) -> date:
    return d - timedelta(days=d.weekday())


def windows(start: date, end: date, window_days: int = DEFAULT_WINDOW_DAYS):
    """
    Monday-aligned [start, end) windows of at most window_days (a multiple of
    7). start is rounded down to its Monday and end up to the next Monday.
    """
    if window_days % 7:
        raise ValueError("window_days must be a multiple of 7")
    s = monday_on_or_before(start)
    e = monday_on_or_before(end - timedelta(days=1)) + timedelta(days=7)
    out = []
    while s < e:
        out.append((s, min(s + timedelta(days=window_days), e)))
        s = out[-1][1]
    return out


def last_complete_week(today: date) -> tuple[date, date]:
    """[Monday, next Monday) of the most recent fully finished ISO week."""
    this_monday = monday_on_or_before(today)
    return this_monday - timedelta(days=7), this_monday


def raw_key(start: date, end: date, run_ts: datetime) -> str:
    stamp = f"{run_ts:%Y%m%dT%H%M%S%f}"
    return f"gdelt/housing_articles/{start:%Y/%m}/{start}_{end}_{stamp}.json"


def extract_to_raw(
    start: date,
    end: date,
    window_days: int = DEFAULT_WINDOW_DAYS,
    max_gb: float = DEFAULT_MAX_GB_PER_QUERY,
    client=None,
    s3=None,
    run_ts: datetime | None = None,
) -> list[str]:
    """Query each window and land its rows + job stats. Returns raw keys."""
    run_ts = run_ts or datetime.now(timezone.utc)
    keys = []
    for ws, we in windows(start, end, window_days):
        rows, stats = fetch_articles(ws, we, max_gb=max_gb, client=client)
        envelope = {
            "source": "gdelt_gkg_bigquery",
            "window_start": ws.isoformat(),
            "window_end": we.isoformat(),
            "fetched_at": run_ts.isoformat(),
            "job": stats,
            "rows": rows,
        }
        key = raw_key(ws, we, run_ts)
        put_raw(key, json.dumps(envelope, default=str).encode(), client=s3)
        keys.append(key)
    return keys


def parse_rows(rows: list[dict]) -> pd.DataFrame:
    """One row per article; lists kept as lists (Delta list<string>)."""
    if not rows:
        return pd.DataFrame(
            columns=["article_id", "published_at", "published_date", "url", "source",
                     "tone", "housing_themes", "zctas", "places"]
        )  # fmt: skip
    df = pd.DataFrame(rows)
    # GDELT timestamps are UTC; stored tz-naive (UTC) like the rest of the lake.
    ts = pd.to_datetime(
        df["gdelt_timestamp"].astype(str), format="%Y%m%d%H%M%S", utc=True
    ).dt.tz_localize(None)
    out = pd.DataFrame(
        {
            "article_id": df["article_id"].astype(str),
            "published_at": ts,
            "published_date": ts.dt.date,
            "url": df["url"].astype(str),
            "source": df["source"].astype(str).str.lower(),
            "tone": pd.to_numeric(df["tone"], errors="coerce"),
            "housing_themes": df["housing_themes"].map(list),
            "zctas": df["zctas"].map(lambda z: [str(x).zfill(5) for x in z]),
            "places": df["places"].map(list),
        }
    )
    return out


def load_raw_to_bronze(
    keys: list[str],
    s3=None,
    table_uri: str | None = None,
    storage_options: dict | None = None,
    run_id: str | None = None,
) -> dict:
    s3 = s3 or get_s3_client()
    bucket = raw_bucket()
    frames, billed_gb = [], 0.0
    for key in keys:
        env = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        df = parse_rows(env["rows"])
        df["source_file"] = f"s3://{bucket}/{key}"
        df["ingested_at"] = datetime.fromisoformat(env["fetched_at"])
        frames.append(df)
        billed_gb += env["job"].get("billed_gb", 0.0)
    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        raise RuntimeError("GDELT returned no housing articles for these windows")
    df["year"] = pd.to_datetime(df["published_date"]).dt.year.astype("int32")
    validation = validate_gdelt_articles(df)
    metrics = merge_to_delta(
        df,
        table_uri or lake_uri("bronze", BRONZE_TABLE),
        keys=BRONZE_KEYS,
        storage_options=(
            storage_options if storage_options is not None else delta_storage_options()
        ),
        partition_by=["year"],
        run_id=run_id,
    )
    metrics.update(
        {
            "validation": validation,
            "articles": int(df["article_id"].nunique()),
            "windows": len(keys),
            "bigquery_billed_gb": round(billed_gb, 2),
            "min_date": str(df["published_date"].min()),
            "max_date": str(df["published_date"].max()),
        }
    )
    return metrics
