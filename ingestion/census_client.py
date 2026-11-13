"""
Census ACS 5-year client: housing + income variables by ZCTA (design doc §5.2).

One request returns every ZCTA for a vintage. The key travels in the URL
query string, so (as in fred_client) errors never include the URL.

Two API quirks handled here:
  - A bad or missing key comes back as an HTML page with HTTP 200, not an
    error status, so the body is checked before parsing.
  - Missing estimates are negative sentinels (-666666666 etc.); they become
    nulls with `is_missing = True`.

ZCTAs approximate zips but are not identical (documented approximation).
"""

import logging
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# variable -> friendly name (design doc §5.2)
ACS_VARIABLES = {
    "B19013_001E": "median_household_income",
    "B01003_001E": "population",
    "B25003_001E": "occupied_units",
    "B25003_003E": "renter_occupied_units",
    "B23025_005E": "unemployed",
    "B25064_001E": "median_gross_rent",
}

GEOGRAPHY = "zip code tabulation area"
REQUEST_TIMEOUT_SECONDS = 180
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 2.0
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

OUTPUT_COLUMNS = ["zcta", "variable", "variable_name", "vintage_year", "value",
                  "is_missing"]  # fmt: skip


class CensusKeyError(ValueError):
    """The Census API rejected (or did not receive) the API key."""


def _key() -> str:
    key = os.getenv("CENSUS_API_KEY")
    if not key:
        raise CensusKeyError("CENSUS_API_KEY not found. Add it to your .env file.")
    return key


def acs5_url(vintage_year: int) -> str:
    return f"https://api.census.gov/data/{vintage_year}/acs/acs5"


def fetch_acs_json(vintage_year: int, zctas: str = "*") -> list[list[str]]:
    """Fetch the ACS variables for all (or the listed) ZCTAs of one vintage."""
    params = {
        "get": ",".join(["NAME", *ACS_VARIABLES]),
        "for": f"{GEOGRAPHY}:{zctas}",
        "key": _key(),
    }
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(
                acs5_url(vintage_year), params=params, timeout=REQUEST_TIMEOUT_SECONDS
            )
        except (requests.ConnectionError, requests.Timeout) as e:
            if attempt == MAX_ATTEMPTS:
                raise requests.ConnectionError(
                    f"Census ACS {vintage_year} request failed after {attempt} "
                    f"attempts: {type(e).__name__}"
                ) from None
            _backoff(vintage_year, attempt, type(e).__name__)
            continue

        if response.status_code in RETRYABLE_STATUS and attempt < MAX_ATTEMPTS:
            _backoff(vintage_year, attempt, f"HTTP {response.status_code}")
            continue

        body = response.text.lstrip()
        if body.startswith("<"):
            # Key problems are an HTML page, usually with HTTP 200.
            title = body.split("<title>", 1)[-1].split("</title>", 1)[0][:40]
            if "Key" in title:
                raise CensusKeyError(
                    f"Census API rejected the key ({title}). Check CENSUS_API_KEY "
                    "(keys are 40 hex characters and must be activated by email)."
                )
            raise requests.HTTPError(
                f"Census ACS {vintage_year}: unexpected HTML response "
                f"(HTTP {response.status_code})"
            )
        if not response.ok:
            raise requests.HTTPError(
                f"Census ACS {vintage_year} failed with HTTP {response.status_code}"
            )
        return response.json()

    raise RuntimeError("unreachable")  # pragma: no cover


def _backoff(vintage_year: int, attempt: int, reason: str) -> None:
    delay = BACKOFF_SECONDS * 2 ** (attempt - 1)
    logger.warning(
        "Census %d: %s on attempt %d, retrying in %.0fs",
        vintage_year,
        reason,
        attempt,
        delay,
    )
    time.sleep(delay)


def parse_acs(rows: list[list[str]], vintage_year: int) -> pd.DataFrame:
    """
    Reshape the API's table (header row + data rows) to long format:
    one row per (zcta, variable, vintage_year).
    """
    if not rows or len(rows) < 2:
        raise ValueError(f"No ZCTA rows in ACS {vintage_year} response")
    header, data = rows[0], rows[1:]
    missing = [c for c in [GEOGRAPHY, *ACS_VARIABLES] if c not in header]
    if missing:
        raise ValueError(f"ACS {vintage_year} response missing columns {missing}")

    wide = pd.DataFrame(data, columns=header)
    long = wide.melt(
        id_vars=[GEOGRAPHY], value_vars=list(ACS_VARIABLES), var_name="variable"
    ).rename(columns={GEOGRAPHY: "zcta"})

    long["zcta"] = long["zcta"].astype(str).str.zfill(5)
    value = pd.to_numeric(long["value"], errors="coerce")
    long["is_missing"] = value.isna() | (value < 0)  # negative = sentinel
    long["value"] = value.where(~long["is_missing"]).astype(float)
    long["variable_name"] = long["variable"].map(ACS_VARIABLES)
    long["vintage_year"] = int(vintage_year)
    return long[OUTPUT_COLUMNS].reset_index(drop=True)
