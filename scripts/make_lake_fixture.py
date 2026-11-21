"""
Write small *synthetic* Bronze Delta tables for tests and CI: every Bronze
table (FRED, Zillow, HUD, Census ACS, BLS CES) and the Spark-produced
silver_spark.zip_lending_activity (HMDA).

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
from ingestion import (  # noqa: E402
    bls_pipeline,
    census_pipeline,
    hud_pipeline,
    zillow_pipeline,
)
from ingestion.bls_client import ces_series_id, parse_payload  # noqa: E402
from ingestion.census_client import ACS_VARIABLES, parse_acs  # noqa: E402
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
                # Real API shape: the zip comes back as "geoid".
                "tract": f"{36061000100 + i}",
                "geoid": z,
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


def build_census_and_bls(lake_root: str, start: str, end: str) -> dict:
    """ACS vintages for every fixture zip; CES employment for its metros."""
    fetched = datetime(2026, 1, 1, tzinfo=timezone.utc)
    acs_frames = []
    first_year, last_year = pd.Timestamp(start).year, pd.Timestamp(end).year
    for vintage in range(first_year - 2, last_year - 1):
        header = ["NAME", *ACS_VARIABLES, "zip code tabulation area"]
        rows = [header]
        for i, (zip_code, info) in enumerate(FIXTURE_ZIPS.items()):
            income = 60000 + 15000 * i + 1000 * (vintage - first_year)
            # 59001's income is suppressed (Census sentinel) to exercise nulls.
            income_cell = "-666666666" if zip_code == "59001" else str(income)
            rows.append(
                [f"ZCTA5 {zip_code}", income_cell, "20000", "8000", "3000",
                 "500", str(info[4] - 100), zip_code]
            )  # fmt: skip
        df = parse_acs(rows, vintage)
        df["source_file"] = f"fixture://census/{vintage}"
        df["ingested_at"] = fetched
        acs_frames.append(df)
    out = {}
    uri = f"{lake_root}/bronze/{census_pipeline.BRONZE_TABLE}"
    Path(uri).mkdir(parents=True, exist_ok=True)
    out["census"] = merge_to_delta(
        pd.concat(acs_frames, ignore_index=True), uri,
        keys=census_pipeline.BRONZE_KEYS, partition_by=["vintage_year"],
        run_id="fixture",
    )  # fmt: skip

    months = pd.date_range(start, end, freq="MS")
    series = {}
    for zip_code, info in FIXTURE_ZIPS.items():
        cbsa = info[3]
        if cbsa == "99999":
            continue
        state_fips = bls_pipeline.STATE_FIPS[info[1]]
        base = 500 + 100 * len(series)
        series[ces_series_id(state_fips, cbsa)] = [
            (str(m.year), f"M{m.month:02d}", f"{base * (1 + 0.02 * i / 12):.1f}", [])
            for i, m in enumerate(months)
        ]
    payload = {"Results": {"series": [
        {"seriesID": sid, "data": [
            {"year": y, "period": p, "value": v, "footnotes": f}
            for y, p, v, f in pts
        ]} for sid, pts in series.items()
    ]}}  # fmt: skip
    df = parse_payload(payload)
    df["source_file"] = "fixture://bls"
    df["ingested_at"] = fetched
    uri = f"{lake_root}/bronze/{bls_pipeline.BRONZE_TABLE}"
    Path(uri).mkdir(parents=True, exist_ok=True)
    out["bls"] = merge_to_delta(
        df, uri, keys=bls_pipeline.BRONZE_KEYS, run_id="fixture"
    )
    return out


def build_lending(lake_root: str, start: str, end: str) -> dict:
    """silver_spark.zip_lending_activity, the shape jobs/hmda_lar.py writes."""
    rows = []
    for year in range(pd.Timestamp(start).year, pd.Timestamp(end).year):
        for i, zip_code in enumerate(FIXTURE_ZIPS):
            loans = 100.0 + 10 * i + 5 * (year % 3)
            investor = round(loans * (0.08 + 0.02 * i), 2)
            rows.append(
                {
                    "activity_year": year, "zip": zip_code,
                    "purchase_loans_est": loans,
                    "investor_purchase_loans_est": investor,
                    "second_home_purchase_loans_est": round(loans * 0.03, 2),
                    "median_loan_amount_est": 250000.0 + 10000 * i,
                    "tracts": 2,
                    "investor_purchase_share": round(investor / loans, 4),
                    "xwalk_vintage": f"{year}Q4", "run_id": "fixture",
                }
            )  # fmt: skip
    uri = f"{lake_root}/silver_spark/zip_lending_activity"
    Path(uri).mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df["tracts"] = df["tracts"].astype("int64")
    df["activity_year"] = df["activity_year"].astype("int32")
    return merge_to_delta(
        df, uri, keys=["zip", "activity_year"], partition_by=["activity_year"],
        run_id="fixture",
    )  # fmt: skip


def build_nri(lake_root: str) -> dict:
    """bronze.fema_nri_raw for the fixture tracts (the shape nri_client writes)."""
    from ingestion import nri_pipeline
    from ingestion.nri_client import PERILS, parse_tracts

    rows = []
    for i, zip_code in enumerate(FIXTURE_ZIPS):
        row = {
            "TRACTFIPS": f"{36061000100 + i}",  # the tract-zip fixture tracts
            "STCOFIPS": "36061",
            "BUILDVALUE": 50_000_000.0,
            "EAL_VALB": 50_000.0 * (i + 1),
            "EAL_SCORE": 10.0 * (i + 1),
        }
        row.update({f"{p}_EALB": 0.0 for p in PERILS})
        row["IFLD_EALB"] = row["EAL_VALB"] * 0.6
        row["HRCN_EALB"] = row["EAL_VALB"] * 0.4
        rows.append(row)
    df = parse_tracts(rows, "2025-12-18")
    df["source_file"] = "fixture://fema_nri"
    df["ingested_at"] = datetime(2026, 1, 1, tzinfo=timezone.utc)
    uri = f"{lake_root}/bronze/{nri_pipeline.BRONZE_TABLE}"
    Path(uri).mkdir(parents=True, exist_ok=True)
    return merge_to_delta(df, uri, keys=nri_pipeline.BRONZE_KEYS, run_id="fixture")


def build_gdelt(lake_root: str) -> dict:
    """bronze.gdelt_housing_articles_raw: a few weeks of synthetic articles."""
    from ingestion import gdelt_pipeline

    rows = []
    for week in range(4):
        for i, zip_code in enumerate(FIXTURE_ZIPS):
            for n in range(3 + i):
                day = 20251201 + week * 7
                rows.append(
                    {
                        "article_id": f"{day}120000-{i}-{n}",
                        "gdelt_timestamp": day * 1_000_000 + 120000,
                        "url": f"https://news.example/{day}/{i}/{n}",
                        "source": "news.example" if n else "prnewswire.com",
                        "tone": -1.0 + 0.5 * i,
                        "housing_themes": ["ECON_HOUSING_PRICES"],
                        "zctas": [zip_code],
                        "places": ["Fixture City"],
                    }
                )
    df = gdelt_pipeline.parse_rows(rows)
    df["source_file"] = "fixture://gdelt"
    df["ingested_at"] = datetime(2026, 1, 1, tzinfo=timezone.utc)
    df["year"] = pd.to_datetime(df["published_date"]).dt.year.astype("int32")
    uri = f"{lake_root}/bronze/{gdelt_pipeline.BRONZE_TABLE}"
    Path(uri).mkdir(parents=True, exist_ok=True)
    return merge_to_delta(
        df, uri, keys=gdelt_pipeline.BRONZE_KEYS, partition_by=["year"],
        run_id="fixture",
    )  # fmt: skip


def build_news(lake_root: str) -> dict:
    """
    bronze.news_events_raw as jobs/stream_news_to_bronze.py writes it: RSS and
    live-GDELT items that exercise each metro-tagging path, the excluded-domain
    filter and cross-source URL dedupe.
    """
    t0 = datetime(2025, 12, 8, 14, 0)
    houston = "Houston, Texas, United States|USTX|29.76|-95.36"
    new_york = "New York, New York, United States|USNY|40.71|-74.0"
    base = {
        "url": None, "title": None, "body_snippet": None, "published_at": t0,
        "fetched_at": t0, "subreddit": None, "feed_name": None, "geo_hint": None,
        "engagement": None, "tone": None, "themes": [],
    }  # fmt: skip
    items = [
        # keyword, bare city name (Houston is bare_ok)
        ("rss", "r1", {"url": "https://www.example.com/a?utm_source=x",
                       "title": "Houston rents cool as supply rises",
                       "body_snippet": "Landlords offer concessions.",
                       "feed_name": "housingwire"}),
        # same article via GDELT (canonical URL matches) -> deduped, snippet kept
        ("gdelt", "g0:1", {"url": "http://example.com/a/",
                           "title": "Houston rents cool as supply rises",
                           "feed_name": "example.com", "tone": -1.5,
                           "geo_hint": houston}),
        # gdelt_geo: city + state code
        ("gdelt", "g1:2", {"url": "https://news.example/ny-1",
                           "title": "Co-op prices climb", "feed_name": "news.example",
                           "tone": 2.0, "themes": ["ECON_HOUSING_PRICES"],
                           "geo_hint": new_york}),
        # keyword with state: "New York, NY"
        ("rss", "r2", {"url": "https://www.example.com/b",
                       "title": "Rent control vote in New York, NY",
                       "feed_name": "redfin_news"}),
        # national: no metro named; no publish time -> fetched_at
        ("rss", "r3", {"url": "https://www.example.com/c",
                       "title": "Mortgage rates fall for a third week",
                       "feed_name": "realtor_research", "published_at": None}),
        # excluded GDELT domain -> dropped in staging
        ("gdelt", "g2:3", {"url": "https://prnewswire.com/x",
                           "title": "Company announces REIT offering",
                           "feed_name": "prnewswire.com", "tone": 3.0}),
    ]  # fmt: skip
    rows = []
    for offset, (source, source_id, fields) in enumerate(items):
        rows.append(
            {
                "source": source, "source_id": source_id, **base, **fields,
                "topic": f"news.{source}.v1", "partition": 0, "offset": offset,
                "kafka_timestamp": t0, "ingested_at": t0,
            }
        )  # fmt: skip
    df = pd.DataFrame(rows)
    uri = f"{lake_root}/bronze/news_events_raw"
    Path(uri).mkdir(parents=True, exist_ok=True)
    return merge_to_delta(
        df, uri, keys=["source", "source_id"], partition_by=["source"],
        run_id="fixture",
    )  # fmt: skip


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
    print(build_census_and_bls(root, args.start, args.end))
    print({"lending": build_lending(root, args.start, args.end)})
    print({"nri": build_nri(root)})
    print({"gdelt": build_gdelt(root)})
    print({"news": build_news(root)})


if __name__ == "__main__":
    main()
