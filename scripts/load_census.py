"""
Load Census ACS 5-year vintages by ZCTA into bronze.census_acs_raw through the
same pipeline as the annual `ingest_census_acs` DAG. Several vintages give
gold_zip_investment_metrics point-in-time history.

    python scripts/load_census.py                         # 2019-2024
    python scripts/load_census.py --vintages 2024
    make load-census VINTAGES="2023 2024"
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.census_pipeline import extract_to_raw, load_raw_to_bronze  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    latest = date.today().year - 2
    parser = argparse.ArgumentParser(description="Load ACS 5-year ZCTA vintages")
    parser.add_argument(
        "--vintages", nargs="+", type=int, default=list(range(2019, latest + 1))
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    for vintage in args.vintages:
        key = extract_to_raw(vintage)
        m = load_raw_to_bronze(key, run_id=f"manual_{date.today():%Y%m%d}")
        print(
            f"ACS {vintage}: {m['zctas']:,} ZCTAs, {m['rows_in']:,} rows "
            f"({m['rows_inserted']:,} new), {m['missing_share']:.1%} suppressed | "
            f"GE {m['validation']['evaluated']} checks passed"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
