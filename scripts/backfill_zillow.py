"""
Load full Zillow ZORI/ZHVI history into Bronze through the same pipeline the
monthly `ingest_zillow` DAG uses: download -> raw zone -> melt -> Great
Expectations -> Delta MERGE on (zip, month). Safe to re-run.

Needs the local stack (`make up`); no API key.

    python scripts/backfill_zillow.py                 # zori + zhvi, full history
    python scripts/backfill_zillow.py --datasets zori
    make backfill-zillow
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.zillow_client import DATASETS  # noqa: E402
from ingestion.zillow_pipeline import extract_to_raw, load_raw_to_bronze  # noqa: E402


def run_backfill(datasets: list[str]) -> list[dict]:
    results = []
    for dataset in datasets:
        print(f"== {dataset}: downloading + landing raw CSV")
        key = extract_to_raw(dataset)
        m = load_raw_to_bronze(
            dataset,
            key,
            lookback_months=None,
            run_id=f"backfill_{date.today():%Y%m%d}",
        )
        print(
            f"   GE {m['validation']['evaluated']} checks passed | "
            f"{m['rows_in']:,} rows ({m['rows_inserted']:,} inserted, "
            f"{m['rows_updated']:,} updated) | {m['zips']:,} zips | "
            f"{m['min_month']} -> {m['max_month']} | {m['duration_s']:.0f}s merge"
        )
        results.append(m)
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill Zillow ZORI/ZHVI to Bronze")
    parser.add_argument(
        "--datasets", nargs="+", choices=sorted(DATASETS), default=sorted(DATASETS)
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    run_backfill(args.datasets)
    return 0


if __name__ == "__main__":
    sys.exit(main())
