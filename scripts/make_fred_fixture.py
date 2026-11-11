"""
Write a small *synthetic* bronze.fred_series_raw Delta table for tests and CI.

The values are fake (smooth made-up curves at each series' real frequency),
but they go through the real parse + merge code, so the schema matches
production Bronze exactly.

    python scripts/make_fred_fixture.py --lake-root ./.ci_lake
    LAKE_ROOT=$PWD/.ci_lake make dbt-build
"""

import argparse
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.bronze_loader import merge_to_delta  # noqa: E402
from ingestion.fred_client import parse_observations  # noqa: E402
from ingestion.fred_pipeline import BRONZE_KEYS, BRONZE_TABLE  # noqa: E402

# series_id -> (pandas frequency, base level, amplitude)
FIXTURE_SERIES = {
    "MORTGAGE30US": ("W-THU", 5.0, 2.0),
    "CUUR0000SAH1": ("MS", 320.0, 40.0),
    "HOUST": ("MS", 1400.0, 200.0),
    "UNRATE": ("MS", 4.5, 1.5),
    "CSUSHPISA": ("MS", 280.0, 50.0),
    "MSPUS": ("QS", 400000.0, 50000.0),
}


def fake_payload(freq: str, base: float, amp: float, start: str, end: str) -> dict:
    dates = pd.date_range(start, end, freq=freq)
    n = max(len(dates) - 1, 1)
    obs = [
        {
            "date": d.strftime("%Y-%m-%d"),
            "value": f"{base + amp * math.sin(i / n * math.pi):.2f}",
        }
        for i, d in enumerate(dates)
    ]
    return {"observations": obs}


def build(lake_root: str, start: str, end: str) -> dict:
    fetched = datetime(2026, 1, 1, tzinfo=timezone.utc)
    frames = []
    for series_id, (freq, base, amp) in FIXTURE_SERIES.items():
        df = parse_observations(
            fake_payload(freq, base, amp, start, end), series_id, ingested_at=fetched
        )
        df["source_file"] = f"fixture://{series_id}"
        frames.append(df)

    uri = f"{lake_root.rstrip('/')}/bronze/{BRONZE_TABLE}"
    Path(uri).mkdir(parents=True, exist_ok=True)
    return merge_to_delta(
        pd.concat(frames, ignore_index=True),
        uri,
        keys=BRONZE_KEYS,
        partition_by=["series_id"],
        run_id="fixture",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--lake-root", required=True)
    parser.add_argument("--start", default="2019-01-01")
    parser.add_argument("--end", default="2025-12-31")
    args = parser.parse_args()
    print(build(str(Path(args.lake_root).resolve()), args.start, args.end))


if __name__ == "__main__":
    main()
