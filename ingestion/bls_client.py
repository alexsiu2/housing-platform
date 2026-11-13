"""
BLS Public API v2 client: CES State and Metro Area employment (design doc §5.2).

Series used: total nonfarm employment, not seasonally adjusted, thousands:
    SMU + state FIPS (2) + area code (5) + 00000000 + 01
For metro areas the CES area code is the CBSA code, so the series for a metro
can be built from the HUD zip->CBSA crosswalk (which gives each metro's
states) instead of BLS's bulk catalog, which needs a contact email in the
User-Agent. Candidates that BLS has no data for are dropped. Micropolitan
areas are not published by CES.

Limits with a key: 50 series and 20 years per request, 500 requests per day.
The key is sent in the JSON body and never appears in errors.
"""

import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
MAX_SERIES_PER_REQUEST = 50
MAX_YEARS_PER_REQUEST = 20
REQUEST_TIMEOUT_SECONDS = 120
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 2.0
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

OUTPUT_COLUMNS = [
    "series_id",
    "state_fips",
    "area_code",
    "month",
    "year",
    "employment_thousands",
    "is_preliminary",
]


class BLSError(RuntimeError):
    """BLS answered, but reported a failure (e.g. bad key, daily limit)."""


def ces_series_id(state_fips: str, area_code: str) -> str:
    """Total nonfarm, all employees, NSA, for one state/area pair."""
    return f"SMU{state_fips:0>2}{area_code:0>5}0000000001"


def _key() -> str:
    key = os.getenv("BLS_API_KEY")
    if not key:
        raise ValueError("BLS_API_KEY not found. Make sure it's set in your .env file.")
    return key


def year_chunks(start_year: int, end_year: int) -> list[tuple[int, int]]:
    """Split a year range into windows the API accepts (<= 20 years each)."""
    if start_year > end_year:
        raise ValueError(f"start_year {start_year} is after end_year {end_year}")
    chunks, y = [], start_year
    while y <= end_year:
        last = min(y + MAX_YEARS_PER_REQUEST - 1, end_year)
        chunks.append((y, last))
        y = last + 1
    return chunks


def fetch_batch(series_ids: list[str], start_year: int, end_year: int) -> dict:
    """POST one request (<= 50 series, <= 20 years). Returns the raw JSON."""
    if not 0 < len(series_ids) <= MAX_SERIES_PER_REQUEST:
        raise ValueError(f"1-{MAX_SERIES_PER_REQUEST} series per request")
    key = _key()
    body = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "registrationkey": key,
    }
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.post(
                BLS_API_URL, json=body, timeout=REQUEST_TIMEOUT_SECONDS
            )
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt == MAX_ATTEMPTS:
                raise requests.ConnectionError(
                    f"BLS request failed after {attempt} attempts: {type(e).__name__}"
                ) from None
            _backoff(attempt, type(e).__name__)
            continue

        if response.status_code in RETRYABLE_STATUS and attempt < MAX_ATTEMPTS:
            _backoff(attempt, f"HTTP {response.status_code}")
            continue
        if not response.ok:
            raise requests.HTTPError(
                f"BLS request failed with HTTP {response.status_code}"
            )

        payload = response.json()
        if payload.get("status") != "REQUEST_SUCCEEDED":
            messages = [m.replace(key, "<key>") for m in payload.get("message", [])]
            raise BLSError(f"BLS status {payload.get('status')}: {messages[:3]}")
        return payload

    raise RuntimeError("unreachable")  # pragma: no cover


def _backoff(attempt: int, reason: str) -> None:
    delay = BACKOFF_SECONDS * 2 ** (attempt - 1)
    logger.warning("BLS: %s on attempt %d, retrying in %.0fs", reason, attempt, delay)
    time.sleep(delay)


def parse_payload(payload: dict) -> pd.DataFrame:
    """
    Flatten a BLS response to one row per (series_id, month).

    Annual averages (period M13) and unparseable values ("-") are dropped.
    Series with no data (unknown or unpublished areas) yield no rows.
    """
    rows = []
    for series in payload.get("Results", {}).get("series", []):
        sid = series["seriesID"]
        for point in series.get("data", []):
            period = point.get("period", "")
            if not (period.startswith("M") and period != "M13"):
                continue
            try:
                value = float(point["value"])
            except (TypeError, ValueError):
                continue
            rows.append(
                {
                    "series_id": sid,
                    "state_fips": sid[3:5],
                    "area_code": sid[5:10],
                    "month": pd.Timestamp(int(point["year"]), int(period[1:]), 1),
                    "year": int(point["year"]),
                    "employment_thousands": value,
                    "is_preliminary": any(
                        f.get("code") == "P" for f in point.get("footnotes") or [] if f
                    ),
                }
            )
    if not rows:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    return pd.DataFrame(rows)[OUTPUT_COLUMNS]


def fetch_series(
    series_ids: list[str], start_year: int, end_year: int | None = None
) -> list[dict]:
    """
    Fetch any number of series over any year range, batching by the API
    limits. Returns the list of raw payloads (one per request).
    """
    end_year = end_year or datetime.now(timezone.utc).year
    unique = list(dict.fromkeys(series_ids))
    payloads = []
    for lo, hi in year_chunks(start_year, end_year):
        for i in range(0, len(unique), MAX_SERIES_PER_REQUEST):
            batch = unique[i : i + MAX_SERIES_PER_REQUEST]
            payloads.append(fetch_batch(batch, lo, hi))
            logger.info("BLS %d-%d: fetched %d series", lo, hi, len(batch))
    return payloads
