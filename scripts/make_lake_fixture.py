"""
Write small *synthetic* Bronze Delta tables for tests and CI:
bronze.fred_series_raw, zori_raw, zhvi_raw and hud_crosswalk_raw.

The values are fake, but they go through the real parse + merge code, so the
schemas match production Bronze exactly.

    python scripts/make_lake_fixture.py --lake-root ./.ci_lake
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
from ingestion import hud_pipeline, zillow_pipeline  # noqa: E402
from ingestion.hud_client import parse_crosswalk  # noqa: E402
from ingestion.zillow_client import parse_wide_csv  # noqa: E402

# zip -> (city, state, metro, primary cbsa, base rent, base home value).
# 01001 checks zero-padding; 59001 sits outside any metro (HUD 99999).
FIXTURE_ZIPS = {
    "77494": (
        "Katy",
        "TX",
        "Houston-The Woodlands-Sugar Land, TX",
        "26420",
        1500,
        300000,
    ),
    "10001": (
        "New York",
        "NY",
        "New York-Newark-Jersey City, NY-NJ",
        "35620",
        4000,
        900000,
    ),
    "01001": ("Agawam", "MA", "Springfield, MA", "44140", 1300, 250000),
    "59001": ("Absarokee", "MT", "", "99999", 900, 200000),
}

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


def zillow_csv(start: str, end: str, value_index: int) -> bytes:
    """A Zillow-shaped wide CSV: id columns + one month-end column per month."""
    months = pd.date_range(start, end, freq="ME")
    header = (
        "RegionID,SizeRank,RegionName,RegionType,StateName,State,City,Metro,"
        "CountyName," + ",".join(f"{m:%Y-%m-%d}" for m in months)
    )
    lines = [header]
    for rank, (zip_code, info) in enumerate(FIXTURE_ZIPS.items()):
        city, state, metro, _, *bases = info
        base = bases[value_index]
        n = max(len(months) - 1, 1)
        values = [f"{base * (1 + 0.3 * i / n):.2f}" for i in range(len(months))]
        # Zillow drops leading zeros in some exports; keep one zip unpadded.
        lines.append(
            f'{9000 + rank},{rank},{zip_code.lstrip("0")},zip,{state},{state},'
            f'{city},"{metro}",Fixture County,' + ",".join(values)
        )
    return ("\n".join(lines) + "\n").encode()


def hud_payload(crosswalk: str) -> dict:
    if crosswalk == "zip-cbsa":
        results = [
            {
                "zip": z,
                "geoid": info[3],
                "res_ratio": 1.0,
                "bus_ratio": 1.0,
                "oth_ratio": 0.0,
                "tot_ratio": 1.0,
                "city": info[0],
                "state": info[1],
            }
            for z, info in FIXTURE_ZIPS.items()
        ]
        # 10001 straddles a second metro with a minority share.
        results[1] = {**results[1], "res_ratio": 0.8, "tot_ratio": 0.8}
        results.append(
            {**results[1], "geoid": "14460", "res_ratio": 0.2, "tot_ratio": 0.2}
        )
    else:
        results = [
            {
                "tract": f"{36061000100 + i}",
                "zip": z,
                "res_ratio": 1.0,
                "bus_ratio": 1.0,
                "oth_ratio": 0.0,
                "tot_ratio": 1.0,
            }
            for i, z in enumerate(FIXTURE_ZIPS)
        ]
    return {"data": {"year": 2025, "quarter": 2, "results": results}}


def build_zillow_and_hud(lake_root: str, start: str, end: str) -> dict:
    fetched = datetime(2026, 1, 1, tzinfo=timezone.utc)
    out = {}
    for dataset, value_index in [("zori", 0), ("zhvi", 1)]:
        df = parse_wide_csv(zillow_csv(start, end, value_index), dataset)
        df["source_file"] = f"fixture://{dataset}"
        df["ingested_at"] = fetched
        uri = f"{lake_root}/bronze/{zillow_pipeline.bronze_table(dataset)}"
        Path(uri).mkdir(parents=True, exist_ok=True)
        out[dataset] = merge_to_delta(
            df,
            uri,
            keys=zillow_pipeline.BRONZE_KEYS,
            partition_by=["year"],
            run_id="fixture",
        )

    frames = []
    for crosswalk in ["zip-cbsa", "tract-zip"]:
        df = parse_crosswalk(hud_payload(crosswalk), crosswalk)
        df["source_file"] = f"fixture://hud/{crosswalk}"
        df["ingested_at"] = fetched
        frames.append(df)
    uri = f"{lake_root}/bronze/{hud_pipeline.BRONZE_TABLE}"
    Path(uri).mkdir(parents=True, exist_ok=True)
    out["hud"] = merge_to_delta(
        pd.concat(frames, ignore_index=True),
        uri,
        keys=hud_pipeline.BRONZE_KEYS,
        partition_by=["crosswalk_type"],
        run_id="fixture",
    )
    return out


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
    root = str(Path(args.lake_root).resolve()).rstrip("/")
    print({"fred": build(root, args.start, args.end)})
    print(build_zillow_and_hud(root, args.start, args.end))


if __name__ == "__main__":
    main()
