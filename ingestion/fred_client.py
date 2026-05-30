import os
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# The five series we care about for this project
FRED_SERIES = {
    "MORTGAGE30US": "30-Year Fixed Mortgage Rate",
    "MSPUS": "Median Sale Price of Houses Sold (US)",
    "HOUST": "Housing Starts (National)",
    "CUUR0000SAH": "CPI - Shelter Component",
    "CSUSHPISA": "Case-Shiller Home Price Index (National)",
}


def fetch_series(
    series_id: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Fetch a single FRED time series and return as a DataFrame.

    Args:
        series_id:  FRED series identifier e.g. 'MORTGAGE30US'
        start_date: 'YYYY-MM-DD' string for observation start
        end_date:   'YYYY-MM-DD' string for observation end

    Returns:
        DataFrame with columns: [date, series_id, value, series_name]
    """
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise ValueError(
            "FRED_API_KEY not found. Make sure it's set in your .env file."
        )

    params = {
        "series_id": series_id,
        "observation_start": start_date,
        "observation_end": end_date,
        "api_key": api_key,
        "file_type": "json",
    }

    response = requests.get(FRED_BASE_URL, params=params)

    # Raise an error if the HTTP request itself failed (e.g. 404, 500)
    response.raise_for_status()

    data = response.json()

    # FRED returns {"observations": [{"date": "...", "value": "."}, ...]}
    # A value of "." means missing data for that period
    observations = data.get("observations", [])
    if not observations:
        raise ValueError(f"No observations returned for series '{series_id}'")

    df = pd.DataFrame(observations)[["date", "value"]]

    # Filter out missing values (FRED uses "." for missing)
    df = df[df["value"] != "."]

    # Cast types
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = df["value"].astype(float)

    # Add metadata columns so we know what series this is later
    df["series_id"] = series_id
    df["series_name"] = FRED_SERIES.get(series_id, series_id)
    df["ingested_at"] = datetime.now(timezone.utc)

    return df[["date", "series_id", "series_name", "value", "ingested_at"]]


def fetch_all_series(
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Fetch all five housing-related FRED series and return
    as a single combined DataFrame.

    Args:
        start_date: 'YYYY-MM-DD'
        end_date:   'YYYY-MM-DD'

    Returns:
        Combined DataFrame of all series stacked vertically
    """
    all_series = []

    for series_id in FRED_SERIES:
        print(f"Fetching {series_id}...")
        try:
            df = fetch_series(series_id, start_date, end_date)
            all_series.append(df)
            print(f"  ✓ {len(df)} observations")
        except Exception as e:
            # Log the error but don't stop — fetch the rest
            print(f"  ✗ Failed to fetch {series_id}: {e}")

    if not all_series:
        raise RuntimeError("Failed to fetch any FRED series.")

    return pd.concat(all_series, ignore_index=True)
