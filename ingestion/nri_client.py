"""
FEMA National Risk Index (NRI) client: expected annual natural-hazard loss by
census tract, from FEMA's own ArcGIS feature service (owner
FEMA_NationalRiskIndex). Used as context on the dashboard, not as a score
input (the owner's decision after sizing it: high-scoring zips do not skew
toward hazard).

What we keep is property loss, not FEMA's composite RISK_SCORE: that score
also folds in social vulnerability and community resilience, which mostly
track income and demographics, not the hazard an owner's building faces.

FEMA's zip downloads on fema.gov answer scripted requests with HTTP 403; the
feature service is the documented programmatic route. Its terms ask users to
"respect" the API, so pages are fetched sequentially with a short pause.
ArcGIS reports errors as JSON with HTTP 200; those are raised here.
"""

import logging
import time
from datetime import datetime, timezone

import pandas as pd
import requests

logger = logging.getLogger(__name__)

ITEM_URL = (
    "https://www.arcgis.com/sharing/rest/content/items/9da4eeb936544335a6db0cd7a8448a51"
)
LAYER_URL = (
    "https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/"
    "National_Risk_Index_Census_Tracts/FeatureServer/0"
)

# Expected annual *building* loss ($) by peril -> readable name.
PERILS = {
    "HRCN": "hurricane",
    "CFLD": "coastal_flooding",
    "IFLD": "inland_flooding",
    "WFIR": "wildfire",
    "TRND": "tornado",
    "SWND": "strong_wind",
    "HAIL": "hail",
    "ERQK": "earthquake",
    "WNTW": "winter_weather",
    "LNDS": "landslide",
}
BASE_FIELDS = ["TRACTFIPS", "STCOFIPS", "BUILDVALUE", "EAL_VALB", "EAL_SCORE"]
FIELDS = BASE_FIELDS + [f"{p}_EALB" for p in PERILS]

PAGE_SIZE = 2000  # the service's maxRecordCount
PAUSE_SECONDS = 0.3
REQUEST_TIMEOUT_SECONDS = 120
MAX_ATTEMPTS = 4
USER_AGENT = "rpof-research/0.1 (personal non-commercial research)"


class ArcGISError(RuntimeError):
    """The feature service answered with an error object."""


def _get(url: str, params: dict) -> dict:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            r = requests.get(
                url,
                params=params,
                headers={"User-Agent": USER_AGENT},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if r.status_code in (429, 500, 502, 503, 504) and attempt < MAX_ATTEMPTS:
                time.sleep(2**attempt)
                continue
            r.raise_for_status()
        except (requests.ConnectionError, requests.Timeout):
            if attempt == MAX_ATTEMPTS:
                raise
            time.sleep(2**attempt)
            continue
        body = r.json()
        if "error" in body:
            raise ArcGISError(f"ArcGIS error: {body['error']}")
        return body
    raise RuntimeError("unreachable")  # pragma: no cover


def release_date() -> str:
    """FEMA's last modification of the dataset, e.g. '2025-12-18'."""
    item = _get(ITEM_URL, {"f": "json"})
    return (
        datetime.fromtimestamp(item["modified"] / 1000, tz=timezone.utc)
        .date()
        .isoformat()
    )


def fetch_all_pages(pause: float = PAUSE_SECONDS) -> list[dict]:
    """Every tract's attributes, paged in OBJECTID order."""
    rows, offset = [], 0
    while True:
        body = _get(
            f"{LAYER_URL}/query",
            {
                "where": "1=1",
                "outFields": ",".join(FIELDS),
                "returnGeometry": "false",
                "orderByFields": "OBJECTID",
                "resultOffset": offset,
                "resultRecordCount": PAGE_SIZE,
                "f": "json",
            },
        )
        feats = body.get("features", [])
        if not feats:
            break
        rows.extend(f["attributes"] for f in feats)
        offset += len(feats)
        logger.info("NRI: fetched %d tracts", offset)
        time.sleep(pause)
    return rows


def parse_tracts(rows: list[dict], release: str) -> pd.DataFrame:
    """One row per tract: building value, expected annual building loss, per peril."""
    if not rows:
        raise ValueError("No tracts in NRI response")
    df = pd.DataFrame(rows)
    missing = [c for c in FIELDS if c not in df.columns]
    if missing:
        raise ValueError(f"NRI response missing fields {missing}")
    out = pd.DataFrame(
        {
            "tract": df["TRACTFIPS"].astype(str).str.zfill(11),
            "county_fips": df["STCOFIPS"].astype(str).str.zfill(5),
            "building_value": pd.to_numeric(df["BUILDVALUE"], errors="coerce"),
            "eal_building": pd.to_numeric(df["EAL_VALB"], errors="coerce"),
            "eal_score": pd.to_numeric(df["EAL_SCORE"], errors="coerce"),
        }
    )
    for code, name in PERILS.items():
        out[f"eal_building_{name}"] = pd.to_numeric(df[f"{code}_EALB"], errors="coerce")
    out["nri_release"] = release
    return out
