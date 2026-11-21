"""
Serving extract for the dashboard: warehouse -> data/dashboard.duckdb.

Why a separate file instead of pointing the dashboard at the warehouse:
  - Format: the warehouse is written by DuckDB 2.0, but Evidence 40.x bundles
    DuckDB 1.4, which can't open 2.0 files. This file is written with
    STORAGE_VERSION v1.4.0.
  - Locking: DuckDB allows one writer or many readers per file. A dashboard
    holding the warehouse open would block dbt_build; this way it never does.

The file is built under a temp name and atomically renamed into place, so a
reader never sees a half-written extract.

    python -m serving.dashboard_export
"""

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from scoring.score import DEFAULT_CONFIG, duckdb_path, load_config

logger = logging.getLogger(__name__)

# Newest on-disk format Evidence's bundled DuckDB (1.4.x) can read.
SERVING_STORAGE_VERSION = "v1.4.0"
HISTORY_START = "2015-01-01"  # ZORI rents start in 2015

# FEMA NRI perils, as named in silver_zip_hazard (eal_rate_<peril>_pct).
PERILS = [
    "hurricane", "coastal_flooding", "inland_flooding", "wildfire", "tornado",
    "strong_wind", "hail", "earthquake", "winter_weather", "landslide",
]  # fmt: skip

# table name -> SQL over the attached warehouse `wh`
TABLES = {
    "scores": "select * from wh.gold.zip_opportunity_score",
    "zip_history": f"""
        select m.zip, m.month, m.rent_index, m.home_value,
               m.gross_rental_yield_pct, m.rent_growth_1yr_pct,
               m.home_value_growth_1yr_pct, m.price_to_income_ratio,
               m.rent_to_income_ratio, m.yoy_job_growth_pct
        from wh.gold.gold_zip_investment_metrics as m
        where m.month >= date '{HISTORY_START}'
          and m.zip in (select zip from wh.gold.zip_opportunity_score)
    """,
    "metro_summary": """
        select
            cbsa_code,
            mode(metro) as metro,
            mode(state) as state,
            count(*) as scored_zips,
            count(*) filter (where tier = 'strong') as strong_zips,
            round(median(opportunity_score), 1) as median_score,
            max(opportunity_score) as best_score,
            arg_max(zip, opportunity_score) as best_zip,
            round(median(gross_rental_yield_pct), 2) as median_yield_pct,
            round(median(rent_growth_1yr_pct), 2) as median_rent_growth_pct,
            round(median(home_value), 0) as median_home_value,
            round(median(rent_index), 0) as median_rent,
            any_value(yoy_job_growth_pct) as job_growth_pct
        from wh.gold.zip_opportunity_score
        where cbsa_code is not null
        group by cbsa_code
    """,
    "zip_lending": """
        select l.zip, l.activity_year, l.purchase_loans_est,
               l.investor_purchase_share, l.purchase_loan_count_yoy_pct,
               l.median_loan_amount_est, l.is_low_sample
        from wh.silver.silver_zip_lending_activity as l
        where l.zip in (select zip from wh.gold.zip_opportunity_score)
    """,
    # FEMA NRI per-peril expected building loss, long format for charting.
    "zip_hazard_perils": f"""
        select zip, peril, eal_rate_pct
        from (
            select zip, {", ".join(f"eal_rate_{p}_pct as {p}" for p in PERILS)}
            from wh.silver.silver_zip_hazard
            where zip in (select zip from wh.gold.zip_opportunity_score)
        ) unpivot (eal_rate_pct for peril in ({", ".join(PERILS)}))
        where eal_rate_pct > 0
    """,
    # GDELT housing news (weekly tone), for the Metro Comparison page.
    "metro_news": """
        select cbsa_code, week_start, article_count, avg_tone, negative_articles,
               positive_articles, is_low_sample, is_national_policy_hub
        from wh.silver.silver_gdelt_metro_tone
        where cbsa_code in (select cbsa_code from wh.gold.zip_opportunity_score)
    """,
    "national_news": """
        select week_start, article_count, avg_tone, negative_articles,
               positive_articles, sources
        from wh.silver.silver_gdelt_national_tone
    """,
    "macro": f"""
        select month, mortgage_rate_30yr, unemployment_rate, housing_starts_k,
               cpi_shelter_yoy_pct, case_shiller_hpi_yoy_pct, median_sale_price
        from wh.silver.silver_macro_indicators
        where month >= date '{HISTORY_START}'
    """,
}


def serving_path() -> str:
    path = os.getenv("SERVING_DUCKDB_PATH")
    if path:
        return path
    return str(Path(duckdb_path()).with_name("dashboard.duckdb"))


def export(
    warehouse: str | None = None,
    target: str | None = None,
    config_path: Path | str = DEFAULT_CONFIG,
) -> dict:
    """Build the serving file. Returns row counts per table."""
    warehouse = warehouse or duckdb_path()
    target = Path(target or serving_path())
    tmp = target.with_name(target.name + ".tmp")
    for stale in (tmp, Path(str(tmp) + ".wal")):
        stale.unlink(missing_ok=True)

    config = load_config(config_path)
    con = duckdb.connect()
    counts = {}
    try:
        con.execute(f"attach '{warehouse}' as wh (read_only)")
        con.execute(
            f"attach '{tmp}' as out (storage_version '{SERVING_STORAGE_VERSION}')"
        )
        for table, sql in TABLES.items():
            con.execute(f"create table out.{table} as {sql}")
            counts[table] = con.execute(f"select count(*) from out.{table}").fetchall()[
                0
            ][0]

        # The active weights, so the dashboard explains the score it shows.
        con.execute(
            "create table out.weights (weights_version varchar, sub_score varchar, "
            "metric varchar, direction varchar, weight double, missing varchar, "
            "required boolean)"
        )
        con.executemany(
            "insert into out.weights values (?, ?, ?, ?, ?, ?, ?)",
            [
                [
                    config.weights_version,
                    s.name,
                    s.metric,
                    "higher" if s.higher_is_better else "lower",
                    s.weight,
                    s.missing,
                    s.name in config.required,
                ]
                for s in config.sub_scores
            ],
        )
        counts["weights"] = len(config.sub_scores)

        con.execute(
            """
            create table out.build_info as
            select any_value(snapshot_month) as snapshot_month,
                   any_value(weights_version) as weights_version,
                   max(scored_at) as scored_at,
                   count(*) as zips_scored,
                   ? as exported_at
            from out.scores
            """,
            [datetime.now(timezone.utc)],
        )
        counts["build_info"] = 1
        con.execute("detach out")
    finally:
        con.close()

    os.replace(tmp, target)  # atomic on the same filesystem
    logger.info("Exported %s -> %s: %s", warehouse, target, counts)
    return {"target": str(target), "tables": counts}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export the dashboard extract")
    parser.add_argument("--warehouse", help="warehouse DuckDB (default: DUCKDB_PATH)")
    parser.add_argument("--target", help="output file (default: data/dashboard.duckdb)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    print(export(args.warehouse, args.target))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
