"""
Data freshness report (design doc §8.1 data_quality_report, §10.1).

For each Bronze table: when it last changed (latest Delta commit), how old
that is, and whether it is overdue (older than 2x its expected cadence).
"""

import logging
from datetime import datetime, timezone

from deltalake import DeltaTable

from ingestion.storage import delta_storage_options, lake_uri

logger = logging.getLogger(__name__)

# Bronze table -> expected update cadence in days (matches the ingest DAGs).
EXPECTED_CADENCE_DAYS = {
    "fred_series_raw": 7,
    "zori_raw": 31,
    "zhvi_raw": 31,
    "bls_ces_raw": 31,
    "hud_crosswalk_raw": 92,
    "census_acs_raw": 366,
}
OVERDUE_FACTOR = 2


def table_freshness(
    table: str, now: datetime | None = None, storage_options: dict | None = None
) -> dict:
    now = now or datetime.now(timezone.utc)
    cadence = EXPECTED_CADENCE_DAYS[table]
    uri = lake_uri("bronze", table)
    opts = storage_options if storage_options is not None else delta_storage_options()
    if not DeltaTable.is_deltatable(uri, storage_options=opts):
        return {"table": table, "status": "missing", "cadence_days": cadence}
    last = DeltaTable(uri, storage_options=opts).history(1)[0]
    updated = datetime.fromtimestamp(last["timestamp"] / 1000, tz=timezone.utc)
    age_days = (now - updated).total_seconds() / 86400
    return {
        "table": table,
        "status": "overdue" if age_days > OVERDUE_FACTOR * cadence else "fresh",
        "last_updated": updated.isoformat(timespec="seconds"),
        "age_days": round(age_days, 1),
        "cadence_days": cadence,
    }


def freshness_report(now: datetime | None = None) -> list[dict]:
    return [table_freshness(t, now) for t in EXPECTED_CADENCE_DAYS]


def problems(report: list[dict]) -> list[str]:
    return [
        f"{r['table']} is {r['status']}"
        + (f" ({r['age_days']}d old, cadence {r['cadence_days']}d)"
           if r["status"] == "overdue" else "")
        for r in report
        if r["status"] != "fresh"
    ]  # fmt: skip
