"""
Zillow Research client: ZORI (rents) and ZHVI (home values) by zip.

Two layers, like fred_client:
  - download_csv(): the raw file bytes (what we land in the raw zone)
  - parse_wide_csv(): wide CSV (one column per month) -> long typed DataFrame

Files are public, no API key. URLs verified 2026-11-10; Zillow updates them
around mid-month.
"""

import io
import logging
import re
import time

import pandas as pd
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://files.zillowstatic.com/research/public_csvs"

# dataset -> (path under BASE_URL, value column name)
DATASETS = {
    # Zillow Observed Rent Index: all homes + multifamily, smoothed, monthly.
    "zori": ("zori/Zip_zori_uc_sfrcondomfr_sm_month.csv", "rent_index"),
    # Zillow Home Value Index: SFR + condo, middle tier, smoothed + seasonally adj.
    "zhvi": ("zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv", "home_value"),
}

ID_COLUMNS = {
    "RegionID": "region_id",
    "SizeRank": "size_rank",
    "RegionName": "zip",
    "RegionType": "region_type",
    "StateName": "state_name",
    "State": "state",
    "City": "city",
    "Metro": "metro",
    "CountyName": "county_name",
}

MONTH_COLUMN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

REQUEST_TIMEOUT_SECONDS = 120
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 2.0
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
USER_AGENT = "rpof-research/0.1 (personal non-commercial research)"


class SchemaError(ValueError):
    """The CSV no longer has the shape this client expects."""


def dataset_url(dataset: str) -> str:
    if dataset not in DATASETS:
        raise ValueError(f"Unknown Zillow dataset '{dataset}'; use {sorted(DATASETS)}")
    return f"{BASE_URL}/{DATASETS[dataset][0]}"


def download_csv(dataset: str) -> bytes:
    """Download one dataset's CSV, retrying timeouts and 429/5xx with backoff."""
    url = dataset_url(dataset)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(
                url,
                timeout=REQUEST_TIMEOUT_SECONDS,
                headers={"User-Agent": USER_AGENT},
            )
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt == MAX_ATTEMPTS:
                raise
            _backoff(dataset, attempt, type(e).__name__)
            continue

        if response.status_code in RETRYABLE_STATUS and attempt < MAX_ATTEMPTS:
            _backoff(dataset, attempt, f"HTTP {response.status_code}")
            continue
        response.raise_for_status()
        logger.info("Downloaded %s: %d bytes", dataset, len(response.content))
        return response.content

    raise RuntimeError("unreachable")  # pragma: no cover


def _backoff(dataset: str, attempt: int, reason: str) -> None:
    delay = BACKOFF_SECONDS * 2 ** (attempt - 1)
    logger.warning(
        "Zillow %s: %s on attempt %d, retrying in %.0fs",
        dataset,
        reason,
        attempt,
        delay,
    )
    time.sleep(delay)


def parse_wide_csv(
    content: bytes, dataset: str, since_month: str | None = None
) -> pd.DataFrame:
    """
    Melt a wide Zillow CSV into one row per (zip, month).

    - `zip` is kept as a zero-padded 5-character string.
    - `month` is the first day of the month (Zillow labels months by their
      last day); `year` is added for partitioning.
    - Empty cells (zip not covered that month) are dropped.
    - `since_month` ('YYYY-MM-DD') keeps only months on or after it, so
      monthly runs can merge a trailing window instead of all history.

    Raises SchemaError if the identifier columns are missing or there are no
    month columns, so a Zillow format change fails loudly.
    """
    value_col = DATASETS[dataset][1]
    wide = pd.read_csv(io.BytesIO(content), dtype={"RegionName": str, "RegionID": str})

    missing = [c for c in ID_COLUMNS if c not in wide.columns]
    if missing:
        raise SchemaError(f"{dataset}: missing identifier columns {missing}")
    month_cols = [c for c in wide.columns if MONTH_COLUMN.match(str(c))]
    if not month_cols:
        raise SchemaError(f"{dataset}: no YYYY-MM-DD month columns found")
    unexpected = set(wide.columns) - set(ID_COLUMNS) - set(month_cols)
    if unexpected:
        logger.warning("%s: ignoring unexpected columns %s", dataset, unexpected)

    if since_month:
        cutoff = pd.Timestamp(since_month)
        month_cols = [c for c in month_cols if pd.Timestamp(c) >= cutoff]

    # Clean identifiers on the wide frame (one row per zip), then make them
    # categoricals so melting ~8M ZHVI rows doesn't copy millions of strings.
    wide = wide[list(ID_COLUMNS) + month_cols].copy()
    wide["RegionName"] = wide["RegionName"].str.strip().str.zfill(5)
    for col in ID_COLUMNS:
        if col != "SizeRank":
            wide[col] = wide[col].astype("category")

    long = wide.melt(
        id_vars=list(ID_COLUMNS), var_name="period_end", value_name=value_col
    )
    long = long.dropna(subset=[value_col]).rename(columns=ID_COLUMNS)

    period_end = pd.to_datetime(long.pop("period_end"), format="%Y-%m-%d")
    long["month"] = period_end.dt.to_period("M").dt.to_timestamp()
    long["year"] = long["month"].dt.year.astype("int32")
    long["size_rank"] = long["size_rank"].astype("int64")
    long[value_col] = long[value_col].astype(float)

    columns = ["zip", "month", "year", value_col] + [
        c for c in ID_COLUMNS.values() if c != "zip"
    ]
    return long[columns].reset_index(drop=True)
