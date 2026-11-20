"""Spark job logic (jobs/hmda_lar.py) on a local Spark session.

Skipped where PySpark/Java aren't installed (e.g. the dev venv); runs in CI's
Spark job and inside the Airflow image.
"""

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from jobs import hmda_lar  # noqa: E402

COLS = list(hmda_lar.LAR_COLUMNS) + ["some_other_column"]


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("test_hmda")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def lar_row(**over):
    row = {c: "" for c in COLS}
    row.update(
        activity_year="2023", lei="X", derived_msa_md="35620", state_code="NY",
        county_code="36061", census_tract="36061000100", action_taken="1",
        loan_type="1", loan_purpose="1", lien_status="1", occupancy_type="1",
        loan_amount="300000", property_value="400000", total_units="1",
        derived_dwelling_category="Single Family (1-4 Units):Site-Built",
        business_or_commercial_purpose="2",
    )  # fmt: skip
    row.update({k: str(v) for k, v in over.items()})
    return row


def raw_df(spark, rows):
    return spark.createDataFrame([[r[c] for c in COLS] for r in rows], COLS)


def test_cast_turns_placeholders_into_nulls(spark):
    df = hmda_lar.select_and_cast(
        raw_df(spark, [lar_row(property_value="Exempt", loan_amount="NA")])
    ).first()
    assert df.property_value is None and df.loan_amount is None
    assert df.activity_year == 2023 and df.census_tract == "36061000100"


def test_cast_fails_loudly_on_schema_change(spark):
    raw = raw_df(spark, [lar_row()]).drop("census_tract")
    with pytest.raises(ValueError, match="census_tract"):
        hmda_lar.select_and_cast(raw)


def test_filter_keeps_only_originated_1_to_4_unit_purchases_in_a_tract(spark):
    rows = [
        lar_row(),  # keep
        lar_row(action_taken=3),  # denied
        lar_row(loan_purpose=31),  # refinance
        lar_row(total_units="5-24"),  # multifamily
        lar_row(census_tract="NA"),  # no tract
        lar_row(total_units="4", occupancy_type=3),  # keep (investor fourplex)
    ]
    kept = hmda_lar.filter_home_purchases(hmda_lar.select_and_cast(raw_df(spark, rows)))
    assert kept.count() == 2


def test_tract_aggregates_count_investor_and_second_homes(spark):
    rows = [
        lar_row(occupancy_type=1, loan_amount=200000),
        lar_row(occupancy_type=3, loan_amount=300000),
        lar_row(occupancy_type=3, loan_amount=400000),
        lar_row(occupancy_type=2, loan_amount=500000),
        lar_row(census_tract="36061000200", occupancy_type=1),
    ]
    lar = hmda_lar.filter_home_purchases(hmda_lar.select_and_cast(raw_df(spark, rows)))
    t = {r.census_tract: r for r in hmda_lar.tract_aggregates(lar).collect()}
    a = t["36061000100"]
    assert (
        a.purchase_loans,
        a.investor_purchase_loans,
        a.second_home_purchase_loans,
    ) == (
        4,
        2,
        1,
    )
    assert a.median_loan_amount in (300000.0, 400000.0)  # approx median of 4 values
    assert t["36061000200"].purchase_loans == 1


def test_allocate_to_zips_splits_by_residential_share(spark):
    tracts = spark.createDataFrame(
        [
            (2023, "T1", "NY", 10, 4, 0, 300000.0),
            (2023, "T2", "NY", 10, 0, 0, 500000.0),
        ],
        "activity_year int, census_tract string, state_code string, "
        "purchase_loans long, investor_purchase_loans long, "
        "second_home_purchase_loans long, median_loan_amount double",
    )
    xwalk = spark.createDataFrame(
        [("T1", "Z1", 0.75), ("T1", "Z2", 0.25), ("T2", "Z2", 1.0), ("T9", "Z9", 1.0)],
        "tract string, zip string, res_ratio double",
    )
    z = {r.zip: r for r in hmda_lar.allocate_to_zips(tracts, xwalk).collect()}
    assert set(z) == {"Z1", "Z2"}  # Z9 has no loans
    assert z["Z1"].purchase_loans_est == 7.5 and z["Z1"].investor_purchase_share == 0.4
    # Z2: 2.5 loans from T1 (1 investor) + 10 from T2 (0 investor)
    assert z["Z2"].purchase_loans_est == 12.5
    assert z["Z2"].investor_purchase_share == 0.08
    assert z["Z2"].median_loan_amount_est == 460000.0  # (300k*2.5 + 500k*10) / 12.5
    assert z["Z2"].tracts == 2


@pytest.mark.parametrize(
    "rates, year, expected",
    [
        # HMDA 2022 uses 2020 tracts; HUD 2022Q4 still had 2010 tracts.
        ({"2022Q4": 0.83, "2023Q4": 0.996, "2026Q2": 0.995}, 2022, "2023Q4"),
        # Equal fit (to 0.1%): closest to the year's Q4 wins.
        ({"2019Q4": 0.9991, "2020Q4": 0.9994, "2023Q4": 0.71}, 2019, "2019Q4"),
        ({"2018Q4": 0.998, "2026Q2": 0.62}, 2018, "2018Q4"),
    ],
)
def test_choose_xwalk_vintage_by_measured_fit(rates, year, expected):
    assert hmda_lar.choose_xwalk_vintage(rates, year) == expected


def test_choose_xwalk_vintage_requires_vintages():
    with pytest.raises(ValueError, match="No tract-zip"):
        hmda_lar.choose_xwalk_vintage({}, 2023)


def test_loan_match_rates(spark):
    tracts = spark.createDataFrame(
        [("T1", 30), ("T2", 70)], "census_tract string, purchase_loans long"
    )
    xwalks = spark.createDataFrame(
        [("A", "T1", "Z", 1.0), ("A", "T2", "Z", 1.0), ("B", "T1", "Z", 1.0),
         ("C", "T9", "Z", 1.0)],
        "vintage string, tract string, zip string, res_ratio double",
    )  # fmt: skip
    assert hmda_lar.loan_match_rates(tracts, xwalks) == {"A": 1.0, "B": 0.3, "C": 0.0}
