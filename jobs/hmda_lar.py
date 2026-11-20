"""
HMDA LAR -> bronze.hmda_lar_raw + silver_spark.zip_lending_activity (design §8.3).

For one activity year:
  1. Read the staged LAR CSV (every US mortgage application; ~99 columns,
     10-25M rows/yr) with all columns as strings, then cast the ones we use.
  2. Keep originated (action_taken=1) home purchases (loan_purpose=1) of 1-4
     unit dwellings with a valid 11-digit census tract -> Bronze, partitioned
     by activity_year/state_code. Re-running a year replaces that year.
  3. Aggregate to tract x year: purchases, investor purchases
     (occupancy_type=3), second-home purchases, median loan amount.
  4. Allocate tracts to zips with the HUD tract->zip crosswalk *for that
     year* (HMDA uses 2010 tracts through 2021 and 2020 tracts from 2022), by
     residential share (res_ratio) -> silver_spark.zip_lending_activity.

Pure DataFrame transformations (no Python UDFs), so driver and executors may
run different Python minor versions. Portable: paths and storage come from
arguments/env, not local assumptions (design §8.3).

    spark-submit jobs/hmda_lar.py --year 2023
"""

import argparse
import json
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Columns kept in Bronze -> Spark SQL type.
LAR_COLUMNS = {
    "activity_year": "int",
    "lei": "string",
    "derived_msa_md": "string",
    "state_code": "string",
    "county_code": "string",
    "census_tract": "string",
    "action_taken": "int",
    "loan_type": "int",
    "loan_purpose": "int",
    "lien_status": "int",
    "occupancy_type": "int",
    "loan_amount": "double",
    "property_value": "double",
    "total_units": "string",
    "derived_dwelling_category": "string",
    "business_or_commercial_purpose": "int",
}
ORIGINATED = 1
HOME_PURCHASE = 1
INVESTMENT = 3  # occupancy_type: 1 principal residence, 2 second home, 3 investment
SECOND_HOME = 2
ONE_TO_FOUR_UNITS = ["1", "2", "3", "4"]


def select_and_cast(raw: DataFrame) -> DataFrame:
    """
    Keep the LAR columns we use and cast them. Non-numeric placeholders
    ("NA", "Exempt") become null instead of failing the job.
    """
    missing = [c for c in LAR_COLUMNS if c not in raw.columns]
    if missing:
        raise ValueError(f"LAR file is missing columns {missing}")
    cols = []
    for name, dtype in LAR_COLUMNS.items():
        col = F.trim(F.col(name))
        if dtype != "string":
            col = F.when(col.rlike(r"^-?\d+(\.\d+)?$"), col).cast(dtype)
        cols.append(col.alias(name))
    return raw.select(*cols)


def filter_home_purchases(lar: DataFrame) -> DataFrame:
    """Originated home-purchase loans on 1-4 unit dwellings in a known tract."""
    return lar.where(
        (F.col("action_taken") == ORIGINATED)
        & (F.col("loan_purpose") == HOME_PURCHASE)
        & F.col("total_units").isin(ONE_TO_FOUR_UNITS)
        & F.col("census_tract").rlike(r"^\d{11}$")
    )


def tract_aggregates(purchases: DataFrame) -> DataFrame:
    """One row per (activity_year, census_tract)."""
    return purchases.groupBy("activity_year", "census_tract").agg(
        F.first("state_code", ignorenulls=True).alias("state_code"),
        F.count("*").alias("purchase_loans"),
        F.sum((F.col("occupancy_type") == INVESTMENT).cast("int")).alias(
            "investor_purchase_loans"
        ),
        F.sum((F.col("occupancy_type") == SECOND_HOME).cast("int")).alias(
            "second_home_purchase_loans"
        ),
        F.percentile_approx("loan_amount", 0.5, 1000).alias("median_loan_amount"),
    )


def allocate_to_zips(tracts: DataFrame, xwalk: DataFrame) -> DataFrame:
    """
    Allocate tract metrics to zips by each tract's residential address share
    in each zip (HUD res_ratio). Counts become estimates (fractional); the
    median loan amount becomes a purchase-weighted average of tract medians.

    xwalk columns: tract, zip, res_ratio.
    """
    j = tracts.join(xwalk, tracts.census_tract == xwalk.tract, "inner")
    w = F.col("res_ratio")
    weighted_purchases = F.col("purchase_loans") * w
    return (
        j.groupBy("activity_year", "zip")
        .agg(
            F.sum(weighted_purchases).alias("purchase_loans_est"),
            F.sum(F.col("investor_purchase_loans") * w).alias(
                "investor_purchase_loans_est"
            ),
            F.sum(F.col("second_home_purchase_loans") * w).alias(
                "second_home_purchase_loans_est"
            ),
            (
                F.sum(F.col("median_loan_amount") * weighted_purchases)
                / F.sum(weighted_purchases)
            ).alias("median_loan_amount_est"),
            F.countDistinct("census_tract").alias("tracts"),
        )
        .where(F.col("purchase_loans_est") > 0)
        .withColumn(
            "investor_purchase_share",
            F.round(
                F.col("investor_purchase_loans_est") / F.col("purchase_loans_est"), 4
            ),
        )
        .withColumn("purchase_loans_est", F.round("purchase_loans_est", 2))
        .withColumn(
            "investor_purchase_loans_est", F.round("investor_purchase_loans_est", 2)
        )
        .withColumn(
            "second_home_purchase_loans_est",
            F.round("second_home_purchase_loans_est", 2),
        )
        .withColumn("median_loan_amount_est", F.round("median_loan_amount_est", 0))
    )


def choose_xwalk_vintage(loan_match_rates: dict[str, float], year: int) -> str:
    """
    Pick the HUD tract->zip vintage for an HMDA year by *measured* fit: the
    share of the year's purchase loans whose tract appears in each vintage.

    Tract boundaries change (HMDA uses 2020 tracts from 2022, but HUD's
    crosswalk only switched in 2023), so "same year" can silently mismatch.
    Highest match rate wins (to 0.1%); ties go to the vintage closest to the
    year's Q4, then the later one.
    """
    if not loan_match_rates:
        raise ValueError("No tract-zip crosswalk vintages in Bronze")

    def distance(v: str) -> int:
        return abs((int(v[:4]) - year) * 4 + (int(v[-1]) - 4))

    return max(
        loan_match_rates,
        key=lambda v: (round(loan_match_rates[v], 3), -distance(v), v),
    )


# --------------------------------------------------------------------------
# I/O (Spark session, Delta, S3A). Not exercised by unit tests.
# --------------------------------------------------------------------------


def build_spark(app_name: str) -> SparkSession:
    endpoint = os.environ.get("STORAGE_ENDPOINT", "http://seaweedfs:8333")
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Keep tables readable by delta-rs/DuckDB: no deletion vectors.
        .config(
            "spark.databricks.delta.properties.defaults.enableDeletionVectors", "false"
        )
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(endpoint.startswith("https")).lower(),
        )
        .config("spark.hadoop.fs.s3a.access.key", os.environ["STORAGE_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["STORAGE_SECRET_KEY"])
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            os.environ.get("STORAGE_REGION", "us-east-1"),
        )
        .config("spark.sql.shuffle.partitions", "16")
    )
    return builder.getOrCreate()


def read_xwalks(spark: SparkSession, lake: str) -> DataFrame:
    hud = spark.read.format("delta").load(f"{lake}/bronze/hud_crosswalk_raw")
    return hud.where(F.col("crosswalk_type") == "tract-zip").select(
        "vintage", F.col("geoid").alias("tract"), "zip",
        F.col("res_ratio").cast("double").alias("res_ratio"),
    )  # fmt: skip


def loan_match_rates(tracts: DataFrame, xwalks: DataFrame) -> dict[str, float]:
    """Share of purchase loans whose tract is in each crosswalk vintage."""
    total = tracts.agg(F.sum("purchase_loans")).first()[0]
    per_vintage = (
        xwalks.select("vintage", "tract").distinct()
        .join(tracts, F.col("tract") == tracts.census_tract)
        .groupBy("vintage").agg(F.sum("purchase_loans").alias("matched"))
        .collect()
    )  # fmt: skip
    rates = {r.vintage: r.matched / total for r in per_vintage}
    for v in xwalks.select("vintage").distinct().collect():
        rates.setdefault(v.vintage, 0.0)
    return rates


def run(
    spark: SparkSession, year: int, staged_csv: str, lake: str, run_id: str
) -> dict:
    raw = spark.read.option("header", True).csv(staged_csv)  # all strings
    lar = select_and_cast(raw)
    purchases = (
        filter_home_purchases(lar)
        .withColumn("source_file", F.lit(staged_csv))
        .withColumn("ingested_at", F.lit(datetime.now(timezone.utc)))
        .withColumn("run_id", F.lit(run_id))
        .cache()
    )
    found_years = [
        r.activity_year for r in purchases.select("activity_year").distinct().collect()
    ]
    if found_years != [year]:
        raise ValueError(f"Expected only activity_year {year}, found {found_years}")

    (
        purchases.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"activity_year = {year}")
        .partitionBy("activity_year", "state_code")
        .save(f"{lake}/bronze/hmda_lar_raw")
    )

    tracts = tract_aggregates(purchases).cache()
    xwalks = read_xwalks(spark, lake).cache()
    rates = loan_match_rates(tracts, xwalks)
    vintage = choose_xwalk_vintage(rates, year)
    xwalk = xwalks.where(F.col("vintage") == vintage).drop("vintage")
    matched = tracts.join(
        xwalk.select("tract").distinct(),
        tracts.census_tract == F.col("tract"),
        "left_semi",
    )

    zips = (
        allocate_to_zips(tracts, xwalk)
        .withColumn("xwalk_vintage", F.lit(vintage))
        .withColumn("run_id", F.lit(run_id))
    )
    (
        zips.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"activity_year = {year}")
        .partitionBy("activity_year")
        .save(f"{lake}/silver_spark/zip_lending_activity")
    )

    totals = tracts.agg(
        F.count("*").alias("tracts"),
        F.sum("purchase_loans").alias("purchases"),
        F.sum("investor_purchase_loans").alias("investor"),
    ).first()
    matched_totals = matched.agg(
        F.count("*").alias("tracts"), F.sum("purchase_loans").alias("purchases")
    ).first()
    summary = {
        "activity_year": year,
        "home_purchase_loans": int(totals.purchases),
        "investor_share_national": round(totals.investor / totals.purchases, 4),
        "tracts": int(totals.tracts),
        "xwalk_vintage": vintage,
        "loan_match_rate_by_vintage": {
            v: round(r, 4) for v, r in sorted(rates.items())
        },
        "tract_match_rate": round(matched_totals.tracts / totals.tracts, 4),
        "loan_match_rate": round((matched_totals.purchases or 0) / totals.purchases, 4),
        "zips": zips.count(),
    }
    return summary


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="HMDA LAR -> Bronze + zip lending activity")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--staged-csv", help="default: s3a://<lake>/staging/hmda/<year>/")
    p.add_argument(
        "--lake", default=f"s3a://{os.environ.get('LAKE_BUCKET', 'rpof-lake')}"
    )
    p.add_argument(
        "--run-id", default=f"manual_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    )
    args = p.parse_args(argv)
    staged = args.staged_csv or f"{args.lake}/staging/hmda/{args.year}/"

    spark = build_spark(f"hmda_lar_{args.year}")
    try:
        summary = run(spark, args.year, staged, args.lake, args.run_id)
    finally:
        spark.stop()
    # One JSON line, easy to find in the Airflow task log.
    print("HMDA_SUMMARY " + json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
