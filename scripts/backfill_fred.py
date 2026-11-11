"""
backfill_fred.py

Load historical FRED macro data through the same pipeline the weekly
`ingest_fred` DAG uses: API -> raw zone (SeaweedFS) -> Great Expectations ->
bronze.fred_series_raw (Delta MERGE). Re-running it, or overlapping a later
scheduled run, never duplicates rows.

Needs the local stack (`make up`) and FRED_API_KEY in .env. Then rebuild
Silver with `make dbt-build`, or let Airflow's dbt_build DAG do it.

Usage:
    python scripts/backfill_fred.py                       # last 5 years
    python scripts/backfill_fred.py --start 2018-01-01 --end 2024-01-01
    python scripts/backfill_fred.py --series UNRATE HOUST
    make backfill-fred START=2019-01-01
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.fred_client import FRED_SERIES  # noqa: E402
from ingestion.fred_pipeline import (  # noqa: E402
    extract_to_raw,
    load_raw_to_bronze,
    resolve_window,
)

DEFAULT_YEARS = 5


def years_before(d: date, years: int) -> date:
    """Same calendar day `years` earlier (Feb 29 falls back to Feb 28)."""
    try:
        return d.replace(year=d.year - years)
    except ValueError:
        return d.replace(year=d.year - years, day=28)


def run_backfill(
    start_date: str, end_date: str, series_ids: list[str] | None = None
) -> dict:
    """Run extract -> validate -> Bronze and print a summary. Returns metrics."""
    series_ids = series_ids or list(FRED_SERIES)

    print("=" * 60)
    print("FRED historical backfill")
    print("=" * 60)
    print(f"Date range : {start_date} -> {end_date}")
    print(f"Series     : {', '.join(series_ids)}")
    print()

    keys = extract_to_raw(start_date, end_date, series_ids)
    print(f"Landed {len(keys)} raw file(s) in the raw zone")

    metrics = load_raw_to_bronze(keys, run_id=f"backfill_{date.today():%Y%m%d}")

    print()
    print(f"Great Expectations : passed {metrics['validation']['evaluated']} checks")
    print(
        f"Bronze MERGE       : {metrics['rows_in']:,} rows in, "
        f"{metrics['rows_inserted']:,} inserted, {metrics['rows_updated']:,} updated"
    )
    print(f"Dates in data      : {metrics['min_date']} -> {metrics['max_date']}")
    print()
    print("Rows per series:")
    for series_id, count in sorted(metrics["rows_per_series"].items()):
        print(f"  {series_id:<14} {count:>6,} observations")

    missing = sorted(set(series_ids) - set(metrics["rows_per_series"]))
    metrics["missing_series"] = missing
    print()
    if missing:
        print(f"WARNING: no data loaded for: {', '.join(missing)}")
    else:
        print(f"All {len(series_ids)} series present")
    print("=" * 60)
    return metrics


def main(argv: list[str] | None = None) -> int:
    today = date.today()
    default_start = years_before(today, DEFAULT_YEARS).isoformat()

    parser = argparse.ArgumentParser(description="Backfill FRED macro data to Bronze")
    parser.add_argument(
        "--start", default=default_start, help=f"YYYY-MM-DD (default {default_start})"
    )
    parser.add_argument(
        "--end", default=today.isoformat(), help="YYYY-MM-DD (default today)"
    )
    parser.add_argument(
        "--series",
        nargs="+",
        choices=sorted(FRED_SERIES),
        metavar="SERIES_ID",
        help="Only these series (default: all tracked series)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    start, end = resolve_window(args.start, args.end)
    metrics = run_backfill(start, end, args.series)
    return 1 if metrics["missing_series"] else 0


if __name__ == "__main__":
    sys.exit(main())
