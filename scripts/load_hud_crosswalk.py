"""
Load HUD USPS crosswalks (zip-cbsa, tract-zip) into bronze.hud_crosswalk_raw
through the same pipeline as the quarterly `ingest_hud_crosswalk` DAG.

Needs HUD_API_TOKEN in .env (free at huduser.gov) and the local stack.

    python scripts/load_hud_crosswalk.py                      # latest vintage
    python scripts/load_hud_crosswalk.py --year 2025 --quarter 2
    make load-hud
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.hud_client import CROSSWALKS  # noqa: E402
from ingestion.hud_pipeline import extract_to_raw, load_raw_to_bronze  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load HUD crosswalks to Bronze")
    parser.add_argument(
        "--crosswalks",
        nargs="+",
        choices=sorted(CROSSWALKS),
        default=sorted(CROSSWALKS),
    )
    parser.add_argument("--year", type=int)
    parser.add_argument("--quarter", type=int, choices=[1, 2, 3, 4])
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    for crosswalk in args.crosswalks:
        key = extract_to_raw(crosswalk, args.year, args.quarter)
        m = load_raw_to_bronze(key, run_id=f"manual_{date.today():%Y%m%d}")
        print(
            f"{crosswalk} {m['vintage']}: {m['rows_in']:,} rows "
            f"({m['rows_inserted']:,} new) | {m['zips']:,} zips, "
            f"{m['geoids']:,} geoids | GE {m['validation']['evaluated']} checks passed"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
