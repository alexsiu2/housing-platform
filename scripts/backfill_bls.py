"""
Load BLS CES metro employment into bronze.bls_ces_raw through the same pipeline
as the monthly `ingest_bls_ces` DAG. Candidate series come from the HUD
zip->CBSA crosswalk already in Bronze (run `make load-hud` first).

    python scripts/backfill_bls.py                  # 2014 -> this year
    python scripts/backfill_bls.py --start 2010
    make backfill-bls
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.bls_pipeline import (  # noqa: E402
    candidate_series,
    extract_to_raw,
    load_raw_to_bronze,
    read_crosswalk,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill BLS CES metro employment")
    parser.add_argument("--start", type=int, default=2014, help="first year")
    parser.add_argument("--end", type=int, default=date.today().year, help="last year")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    candidates = candidate_series(read_crosswalk())
    print(f"{len(candidates):,} candidate (metro, state) series")
    keys = extract_to_raw(args.start, args.end, series_ids=candidates)
    m = load_raw_to_bronze(keys, run_id=f"backfill_{date.today():%Y%m%d}")
    print(
        f"{m['series_with_data']:,} series with data ({m['areas']:,} metros) | "
        f"{m['rows_in']:,} rows ({m['rows_inserted']:,} new) | "
        f"{m['min_month']} -> {m['max_month']} | "
        f"GE {m['validation']['evaluated']} checks passed"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
