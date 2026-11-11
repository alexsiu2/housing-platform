"""
FRED (Federal Reserve Economic Data) API client.

Two layers:
  - fetch_series_json(): the raw API payload (what we land in the raw zone)
  - parse_observations(): payload -> typed DataFrame (what we load to Bronze)

fetch_series() combines both for convenience.
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

# urllib3 logs full request URLs at DEBUG, and FRED puts the API key in the
# query string. Keep it at WARNING so turning on debug logging can't leak it.
logging.getLogger("urllib3").setLevel(logging.WARNING)

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# The macro series this project tracks (design doc §5.2).
FRED_SERIES = {
    "MORTGAGE30US": "30-Year Fixed Mortgage Rate",
    "CUUR0000SAH1": "CPI - Shelter Component",
    "HOUST": "Housing Starts (National)",
    "UNRATE": "Unemployment Rate",
    "CSUSHPISA": "Case-Shiller Home Price Index (National)",
    "MSPUS": "Median Sale Price of Houses Sold (US)",
}

REQUEST_TIMEOUT_SECONDS = 30
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 2.0
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

OUTPUT_COLUMNS = ["date", "series_id", "series_name", "value", "ingested_at"]


def _api_key() -> str:
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise ValueError(
            "FRED_API_KEY not found. Make sure it's set in your .env file."
        )
    return api_key


def fetch_series_json(series_id: str, start_date: str, end_date: str) -> dict:
    """
    Fetch the raw FRED observations payload for one series.

    Retries timeouts, connection errors and 429/5xx responses with
    exponential backoff. Error messages never include the request URL,
    because FRED puts the API key in the query string.
    """
    params = {
        "series_id": series_id,
        "observation_start": start_date,
        "observation_end": end_date,
        "api_key": _api_key(),
        "file_type": "json",
    }

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(
                FRED_BASE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS
            )
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt == MAX_ATTEMPTS:
                raise requests.ConnectionError(
                    f"FRED request for {series_id} failed after {attempt} "
                    f"attempts: {type(e).__name__}"
                ) from None
            _backoff(series_id, attempt, type(e).__name__)
            continue

        if response.status_code in RETRYABLE_STATUS and attempt < MAX_ATTEMPTS:
            _backoff(series_id, attempt, f"HTTP {response.status_code}")
            continue

        try:
            response.raise_for_status()
        except requests.HTTPError:
            raise requests.HTTPError(
                f"FRED request for {series_id} failed with HTTP "
                f"{response.status_code}"
            ) from None
        return response.json()

    raise RuntimeError("unreachable")  # pragma: no cover


def _backoff(series_id: str, attempt: int, reason: str) -> None:
    delay = BACKOFF_SECONDS * 2 ** (attempt - 1)
    logger.warning(
        "FRED %s: %s on attempt %d, retrying in %.0fs",
        series_id,
        reason,
        attempt,
        delay,
    )
    time.sleep(delay)


def parse_observations(
    payload: dict, series_id: str, ingested_at: datetime | None = None
) -> pd.DataFrame:
    """
    Turn a FRED observations payload into a typed DataFrame.

    Returns columns: [date, series_id, series_name, value, ingested_at].
    FRED marks missing values with "."; those rows are dropped.
    """
    observations = payload.get("observations", [])
    if not observations:
        raise ValueError(f"No observations returned for series '{series_id}'")

    df = pd.DataFrame(observations)[["date", "value"]]
    df = df[df["value"] != "."].copy()

    df["date"] = pd.to_datetime(df["date"])
    df["value"] = df["value"].astype(float)
    df["series_id"] = series_id
    df["series_name"] = FRED_SERIES.get(series_id, series_id)
    df["ingested_at"] = ingested_at or datetime.now(timezone.utc)

    return df[OUTPUT_COLUMNS].reset_index(drop=True)


def fetch_series(series_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch a single FRED time series and return it as a DataFrame.

    Args:
        series_id:  FRED series identifier e.g. 'MORTGAGE30US'
        start_date: 'YYYY-MM-DD' string for observation start
        end_date:   'YYYY-MM-DD' string for observation end

    Returns:
        DataFrame with columns: [date, series_id, series_name, value, ingested_at]
    """
    payload = fetch_series_json(series_id, start_date, end_date)
    return parse_observations(payload, series_id)


def fetch_all_series(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch every series in FRED_SERIES and stack them into one DataFrame.

    A failure on one series is logged and skipped; if all fail, raises.
    """
    all_series = []

    for series_id in FRED_SERIES:
        try:
            df = fetch_series(series_id, start_date, end_date)
            all_series.append(df)
            logger.info("Fetched %s: %d observations", series_id, len(df))
        except Exception as e:
            logger.error("Failed to fetch %s: %s", series_id, e)

    if not all_series:
        raise RuntimeError("Failed to fetch any FRED series.")

    return pd.concat(all_series, ignore_index=True)
