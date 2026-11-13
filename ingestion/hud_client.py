"""
HUD USPS ZIP crosswalk API client (design doc §5.2).

Two crosswalks:
  - zip-cbsa   (type 3): connects zip-level data to metro (CBSA) data
  - tract-zip  (type 6): allocates tract-level data (HMDA) to zips

Each row carries address-share ratios (res_ratio = residential share, etc.).
Requires a free token from huduser.gov in HUD_API_TOKEN, sent as a Bearer
header. Like the FRED client, errors never include the token.

Response shapes (verified against the live API, vintage 2026Q2):
  zip-cbsa:  {"zip": "00501", "geoid": "35620", ...ratios, "city", "state"}
  tract-zip: {"tract": "36103158607", "geoid": "00501", ...}  <- zip is geoid
"""

import logging
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

HUD_API_URL = "https://www.huduser.gov/hudapi/public/usps"

# crosswalk name -> HUD API "type" parameter
CROSSWALKS = {"zip-cbsa": 3, "tract-zip": 6}

RATIO_COLUMNS = ["res_ratio", "bus_ratio", "oth_ratio", "tot_ratio"]
OUTPUT_COLUMNS = [
    "crosswalk_type",
    "vintage",
    "year",
    "quarter",
    "zip",
    "geoid",
    *RATIO_COLUMNS,
    "city",
    "state",
]

REQUEST_TIMEOUT_SECONDS = 120
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 2.0
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def _token() -> str:
    token = os.getenv("HUD_API_TOKEN")
    if not token:
        raise ValueError(
            "HUD_API_TOKEN not found. Create a token at huduser.gov and add it "
            "to your .env file."
        )
    return token


def fetch_crosswalk_json(
    crosswalk: str, year: int | None = None, quarter: int | None = None
) -> dict:
    """
    Fetch one full crosswalk (query=All). Omit year/quarter for the latest.

    Retries timeouts and 429/5xx with exponential backoff.
    """
    if crosswalk not in CROSSWALKS:
        raise ValueError(f"Unknown crosswalk '{crosswalk}'; use {sorted(CROSSWALKS)}")
    params = {"type": CROSSWALKS[crosswalk], "query": "All"}
    if year:
        params["year"] = year
    if quarter:
        params["quarter"] = quarter
    headers = {"Authorization": f"Bearer {_token()}"}

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(
                HUD_API_URL,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt == MAX_ATTEMPTS:
                raise requests.ConnectionError(
                    f"HUD {crosswalk} request failed after {attempt} attempts: "
                    f"{type(e).__name__}"
                ) from None
            _backoff(crosswalk, attempt, type(e).__name__)
            continue

        if response.status_code in RETRYABLE_STATUS and attempt < MAX_ATTEMPTS:
            _backoff(crosswalk, attempt, f"HTTP {response.status_code}")
            continue
        if not response.ok:
            raise requests.HTTPError(
                f"HUD {crosswalk} request failed with HTTP {response.status_code}"
            )
        return response.json()

    raise RuntimeError("unreachable")  # pragma: no cover


def _backoff(crosswalk: str, attempt: int, reason: str) -> None:
    delay = BACKOFF_SECONDS * 2 ** (attempt - 1)
    logger.warning(
        "HUD %s: %s on attempt %d, retrying in %.0fs",
        crosswalk,
        reason,
        attempt,
        delay,
    )
    time.sleep(delay)


def parse_crosswalk(payload: dict, crosswalk: str) -> pd.DataFrame:
    """
    Normalize a crosswalk payload to one row per (zip, geoid).

    `geoid` is the CBSA code for zip-cbsa and the 11-digit tract for tract-zip.
    `vintage` is 'YYYYQn' from the response, e.g. '2025Q2'.
    """
    data = payload.get("data", payload)
    results = data.get("results") or []
    if not results:
        raise ValueError(f"No results in HUD {crosswalk} response")

    df = pd.DataFrame(results)
    if crosswalk == "tract-zip":
        # Verified 2026Q2: tract-zip rows are {"tract": <tract>, "geoid": <zip>}
        # with no "zip" key. Normalize to zip=<zip>, geoid=<tract>.
        if "tract" not in df.columns:
            raise ValueError("HUD tract-zip response missing fields ['tract']")
        df = df.rename(columns={"geoid": "zip", "tract": "geoid"})
    missing = [c for c in ["zip", "geoid", "res_ratio"] if c not in df.columns]
    if missing:
        raise ValueError(f"HUD {crosswalk} response missing fields {missing}")

    year, quarter = int(data["year"]), int(data["quarter"])
    df["crosswalk_type"] = crosswalk
    df["year"] = year
    df["quarter"] = quarter
    df["vintage"] = f"{year}Q{quarter}"
    df["zip"] = df["zip"].astype(str).str.strip().str.zfill(5)
    df["geoid"] = df["geoid"].astype(str).str.strip()
    for col in RATIO_COLUMNS:
        df[col] = pd.to_numeric(df.get(col), errors="coerce").astype(float)
    for col in ["city", "state"]:
        if col not in df.columns:
            df[col] = None

    return df[OUTPUT_COLUMNS].reset_index(drop=True)
