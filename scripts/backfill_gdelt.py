"""
Backfill US housing news from GDELT (BigQuery) into
bronze.gdelt_housing_articles_raw, in Monday-aligned 4-week windows.

Prints the dry-run cost of every window first (free) and stops if the total
exceeds --max-total-gb, before scanning anything.

    python scripts/backfill_gdelt.py --start 2024-01-01            # to last full week
    python scripts/backfill_gdelt.py --start 2026-10-01 --end 2026-11-16
    make backfill-gdelt START=2024-01-01
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.gdelt_client import estimate_gb, get_client  # noqa: E402
from ingestion.gdelt_pipeline import (  # noqa: E402
    extract_to_raw,
    last_complete_week,
    load_raw_to_bronze,
    windows,
)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Backfill GDELT housing news via BigQuery")
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument(
        "--end", type=date.fromisoformat, help="default: end of last full week"
    )
    p.add_argument("--max-total-gb", type=float, default=400.0)
    p.add_argument("--dry-run", action="store_true", help="only print the cost")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    end = args.end or last_complete_week(date.today())[1]

    client = get_client()
    plan = windows(args.start, end)
    est = [estimate_gb(s, e, client=client) for s, e in plan]
    total = sum(est)
    print(f"{len(plan)} windows {plan[0][0]} -> {plan[-1][1]}: ~{total:.0f} GB scanned "
          f"(max window {max(est):.1f} GB)")  # fmt: skip
    if total > args.max_total_gb:
        print(f"Refusing: over --max-total-gb {args.max_total_gb:.0f}")
        return 1
    if args.dry_run:
        return 0

    loaded = 0
    for s, e in plan:  # one window at a time: land, then load (resumable)
        keys = extract_to_raw(s, e, client=client)
        m = load_raw_to_bronze(keys, run_id=f"backfill_{date.today():%Y%m%d}")
        loaded += m["articles"]
        print(f"  {s} -> {e}: {m['articles']:,} articles, "
              f"{m['bigquery_billed_gb']:.1f} GB billed")  # fmt: skip
    print(f"Done: {loaded:,} articles loaded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
