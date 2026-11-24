from datetime import date, datetime, timezone

import duckdb
import pandas as pd
import pytest

from scoring import score
from serving import dashboard_export


@pytest.fixture
def warehouse(tmp_path):
    """A tiny warehouse with the gold + silver tables the export reads."""
    path = str(tmp_path / "wh.duckdb")
    con = duckdb.connect(path)
    for schema in ("gold", "silver"):
        con.execute(f"create schema {schema}")
    scores = pd.DataFrame(
        {
            "zip": ["00001", "00002", "00003"],
            "snapshot_month": [date(2026, 9, 1)] * 3,
            "national_rank": [1, 2, 3],
            "opportunity_score": [90.0, 60.0, 30.0],
            "tier": ["strong", "moderate", "weak"],
            "cbsa_code": ["A", "A", None],
            "metro": ["Metro A", "Metro A", None],
            "state": ["TX", "TX", "MT"],
            "gross_rental_yield_pct": [9.0, 6.0, 4.0],
            "rent_growth_1yr_pct": [5.0, 3.0, 1.0],
            "home_value": [150000.0, 250000.0, 300000.0],
            "rent_index": [1200.0, 1300.0, 1000.0],
            "yoy_job_growth_pct": [1.5, 1.5, None],
            "weights_version": ["v1.1"] * 3,
            "scored_at": [datetime(2026, 11, 13, tzinfo=timezone.utc)] * 3,
        }
    )
    history = pd.DataFrame(
        {
            "zip": ["00001", "00001", "00002", "99999"],
            "month": pd.to_datetime(
                ["2014-12-01", "2026-09-01", "2026-09-01", "2026-09-01"]
            ),
            **{
                c: [1.0] * 4
                for c in [
                    "rent_index", "home_value", "gross_rental_yield_pct",
                    "rent_growth_1yr_pct", "home_value_growth_1yr_pct",
                    "price_to_income_ratio", "rent_to_income_ratio",
                    "yoy_job_growth_pct",
                ]
            },  # fmt: skip
        }
    )
    macro = pd.DataFrame(
        {
            "month": pd.to_datetime(["2014-06-01", "2026-09-01"]),
            **{
                c: [1.0, 2.0]
                for c in [
                    "mortgage_rate_30yr", "unemployment_rate", "housing_starts_k",
                    "cpi_shelter_yoy_pct", "case_shiller_hpi_yoy_pct",
                    "median_sale_price",
                ]
            },  # fmt: skip
        }
    )
    lending = pd.DataFrame(
        {
            "zip": ["00001", "00001", "99999"],
            "activity_year": [2023, 2024, 2024],
            "purchase_loans_est": [100.0, 110.0, 5.0],
            "investor_purchase_share": [0.1, 0.12, 0.5],
            "purchase_loan_count_yoy_pct": [None, 10.0, None],
            "median_loan_amount_est": [250000.0, 260000.0, 1.0],
            "is_low_sample": [False, False, True],
        }
    )
    hazard = pd.DataFrame(
        {
            "zip": ["00001", "99999"],
            **{
                f"eal_rate_{p}_pct": [0.1, 0.2]
                for p in [
                    "hurricane",
                    "coastal_flooding",
                    "inland_flooding",
                    "wildfire",
                    "tornado",
                    "strong_wind",
                    "hail",
                    "earthquake",
                    "winter_weather",
                    "landslide",
                ]
            },
        }
    )
    hazard.loc[0, "eal_rate_wildfire_pct"] = 0.0  # zero rows are dropped
    metro_news = pd.DataFrame(
        {
            "cbsa_code": ["A", "ZZ"],
            "week_start": pd.to_datetime(["2026-11-09", "2026-11-09"]),
            "article_count": [12, 3],
            "avg_tone": [-0.5, 1.0],
            "negative_articles": [3, 0],
            "positive_articles": [2, 1],
            "is_low_sample": [False, True],
            "is_national_policy_hub": [False, False],
        }
    )
    national_news = pd.DataFrame(
        {
            "week_start": pd.to_datetime(["2026-11-09"]),
            "article_count": [4500],
            "avg_tone": [-0.4],
            "negative_articles": [900],
            "positive_articles": [700],
            "sources": [1800],
        }
    )
    for name, df, schema in [
        ("silver_gdelt_metro_tone", metro_news, "silver"),
        ("silver_gdelt_national_tone", national_news, "silver"),
        ("silver_zip_hazard", hazard, "silver"),
        ("silver_zip_lending_activity", lending, "silver"),
        ("zip_opportunity_score", scores, "gold"),
        ("gold_zip_investment_metrics", history, "gold"),
        ("silver_macro_indicators", macro, "silver"),
    ]:
        con.register("df", df)
        con.execute(f"create table {schema}.{name} as select * from df")
        con.unregister("df")
    con.close()
    return path


def _read(path, sql):
    con = duckdb.connect(path, read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_export_builds_every_table(warehouse, tmp_path):
    target = str(tmp_path / "dashboard.duckdb")
    out = dashboard_export.export(warehouse, target)
    assert out["tables"] == {
        "scores": 3,
        "zip_history": 2,  # pre-2015 row and unscored zip dropped
        "metro_summary": 1,  # zips without a CBSA excluded
        "zip_lending": 2,  # unscored zip dropped
        "zip_hazard_perils": 9,  # 10 perils for 00001, minus the zero one
        "metro_news": 1,  # metro without scored zips dropped
        "national_news": 1,
        "macro": 1,  # pre-2015 dropped
        "weights": 5,
        "build_info": 1,
        # No LLM outlook run yet: exported empty, with their columns.
        "news_outlook": 0,
        "news_outlook_citations": 0,
    }
    assert _read(target, "select cbsa_code, scored_zips, strong_zips, best_zip "
                 "from metro_summary") == [("A", 2, 1, "00001")]  # fmt: skip
    info = _read(target, "select weights_version, zips_scored from build_info")
    assert info == [("v1.1", 3)]


def test_weights_table_matches_live_config(warehouse, tmp_path):
    target = str(tmp_path / "dashboard.duckdb")
    dashboard_export.export(warehouse, target)
    rows = _read(target, "select sub_score, metric, weight, missing, required "
                 "from weights order by sub_score")  # fmt: skip
    config = score.load_config()
    expected = sorted(
        (s.name, s.metric, s.weight, s.missing, s.name in config.required)
        for s in config.sub_scores
    )
    assert rows == expected
    assert sum(r[2] for r in rows) == pytest.approx(1)


def test_export_replaces_atomically_and_leaves_no_temp(warehouse, tmp_path):
    target = tmp_path / "dashboard.duckdb"
    target.write_bytes(b"old extract")
    dashboard_export.export(warehouse, str(target))
    assert _read(str(target), "select count(*) from scores") == [(3,)]
    assert not list(tmp_path.glob("dashboard.duckdb.tmp*"))


def test_export_never_writes_to_the_warehouse(warehouse, tmp_path):
    before = _read(warehouse, "select count(*) from duckdb_tables()")
    dashboard_export.export(warehouse, str(tmp_path / "d.duckdb"))
    assert _read(warehouse, "select count(*) from duckdb_tables()") == before


def test_serving_path_defaults_next_to_warehouse(monkeypatch, tmp_path):
    monkeypatch.delenv("SERVING_DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "rpof.duckdb"))
    assert dashboard_export.serving_path() == str(tmp_path / "dashboard.duckdb")
    monkeypatch.setenv("SERVING_DUCKDB_PATH", "/x/y.duckdb")
    assert dashboard_export.serving_path() == "/x/y.duckdb"


def test_export_includes_llm_outlooks_when_present(warehouse, tmp_path):
    from llm import summarize as s

    con = duckdb.connect(warehouse)
    con.execute(s.OUTLOOK_DDL)
    con.execute(s.CITATIONS_DDL)
    con.execute(
        f"insert into {s.OUTLOOK_TABLE} (cbsa_code, week_start, outlook, key_themes) "
        "values ('A', date '2026-11-16', 'neutral', ['supply', 'rates'])"
    )
    con.close()
    target = str(tmp_path / "dashboard.duckdb")
    out = dashboard_export.export(warehouse, target)
    assert out["tables"]["news_outlook"] == 1
    themes = _read(target, "select key_themes from news_outlook")
    assert themes == [("supply · rates",)]  # text: Evidence can't load lists
