from pathlib import Path

import duckdb
import pandas as pd
import pytest

from scoring import score

MONTH = pd.Timestamp("2026-09-01")
CONFIG = score.load_config()


def metrics(rows):
    """Rows of (zip, yield, rent_growth, rent_to_income, price_to_rent, jobs)."""
    df = pd.DataFrame(
        rows,
        columns=[
            "zip",
            "gross_rental_yield_pct",
            "rent_growth_1yr_pct",
            "rent_to_income_ratio",
            "price_to_rent_ratio",
            "yoy_job_growth_pct",
        ],
    )
    df["month"] = MONTH
    df["cbsa_code"] = ["A", "A", "B", "B", "C"][: len(df)]
    df["metro"] = df["cbsa_code"]
    df["state"], df["city"] = "TX", "X"
    df["rent_index"], df["home_value"] = 1500.0, 300000.0
    df["median_household_income"], df["population"] = 70000.0, 5000.0
    df["demographics_vintage"] = 2024
    df["job_growth_month"] = MONTH
    return df


FIVE = [
    ("00001", 8.0, 5.0, 0.20, 12.0, 3.0),  # best on everything
    ("00002", 6.0, 3.0, 0.30, 16.0, 2.0),
    ("00003", 5.0, 2.0, 0.35, 20.0, 1.0),
    ("00004", 4.0, 1.0, 0.40, 25.0, 0.0),
    ("00005", 3.0, -1.0, 0.50, 33.0, -1.0),  # worst on everything
]


def test_config_loads_and_weights_sum_to_one():
    assert CONFIG.weights_version == "v1.0"
    assert sum(s.weight for s in CONFIG.sub_scores) == pytest.approx(1)
    assert set(CONFIG.required) == {
        "score_rental_yield",
        "score_rent_growth",
        "score_affordability",
    }


def test_bad_config_rejected(tmp_path):
    bad = Path(score.DEFAULT_CONFIG).read_text().replace("weight: 0.30", "weight: 0.50")
    (tmp_path / "s.yaml").write_text(bad)
    with pytest.raises(ValueError, match="sum to 1"):
        score.load_config(tmp_path / "s.yaml")


def test_percentile_score_directions_ties_and_nulls():
    s = pd.Series([10.0, 20.0, 20.0, 40.0, None])
    hi = score.percentile_score(s, higher_is_better=True)
    lo = score.percentile_score(s, higher_is_better=False)
    assert hi.tolist()[:4] == [
        0.0,
        pytest.approx(100 / 3),
        pytest.approx(100 / 3),
        100.0,
    ]
    assert lo.tolist()[:4] == [
        100.0,
        pytest.approx(100 / 3),
        pytest.approx(100 / 3),
        0.0,
    ]
    assert pd.isna(hi.iloc[4])


def test_best_and_worst_zips_rank_and_tier_correctly():
    out = score.compute_scores(metrics(FIVE), CONFIG, MONTH)
    first, last = out.iloc[0], out.iloc[-1]
    assert (first["zip"], first["opportunity_score"], first["tier"]) == (
        "00001",
        100.0,
        "strong",
    )
    assert (last["zip"], last["opportunity_score"], last["tier"]) == (
        "00005",
        0.0,
        "weak",
    )
    assert out["national_rank"].tolist() == [1, 2, 3, 4, 5]
    assert out.loc[out.zip == "00003", "opportunity_score"].item() == 50.0
    assert out["weights_version"].unique().tolist() == ["v1.0"]
    score.check_scores(out, CONFIG)


def test_lower_is_better_metrics_are_inverted():
    out = score.compute_scores(metrics(FIVE), CONFIG, MONTH).set_index("zip")
    # 00001 has the lowest rent-to-income and price-to-rent: top sub-scores.
    assert out.loc["00001", "score_affordability"] == 100.0
    assert out.loc["00001", "score_entry_price"] == 100.0
    assert out.loc["00005", "score_affordability"] == 0.0


def test_missing_job_growth_redistributes_weight_and_flags_partial():
    rows = [list(r) for r in FIVE]
    rows[0][5] = None  # best zip has no metro job data
    out = score.compute_scores(metrics(rows), CONFIG, MONTH).set_index("zip")
    best = out.loc["00001"]
    assert bool(best["partial_score"]) is True
    assert pd.isna(best["score_demand_growth"])
    # All other sub-scores are 100 -> redistributed composite is still 100.
    assert best["opportunity_score"] == 100.0
    assert not out.drop(index="00001")["partial_score"].any()


def test_stale_job_data_treated_as_missing():
    df = metrics(FIVE)
    df.loc[df.zip == "00002", "job_growth_month"] = MONTH - pd.DateOffset(months=4)
    out = score.compute_scores(df, CONFIG, MONTH).set_index("zip")
    assert bool(out.loc["00002", "partial_score"]) is True
    assert not out.loc["00003", "partial_score"]


def test_required_metric_missing_or_tiny_population_excluded():
    df = metrics(FIVE)
    df.loc[df.zip == "00002", "rent_growth_1yr_pct"] = None  # e.g. <12 months history
    df.loc[df.zip == "00004", "population"] = 300.0
    out = score.compute_scores(df, CONFIG, MONTH)
    assert set(out["zip"]) == {"00001", "00003", "00005"}
    assert out["national_rank"].tolist() == [1, 2, 3]


def test_metro_rank_within_cbsa():
    out = score.compute_scores(metrics(FIVE), CONFIG, MONTH).set_index("zip")
    assert out.loc["00001", "metro_rank"] == 1 and out.loc["00002", "metro_rank"] == 2
    assert out.loc["00002", "metro_zip_count"] == 2


def test_check_scores_catches_problems():
    out = score.compute_scores(metrics(FIVE), CONFIG, MONTH)
    broken = out.copy()
    broken.loc[0, "opportunity_score"] = 130
    broken.loc[1, "national_rank"] = 7
    with pytest.raises(score.ScoreCheckError, match="outside 0-100.*not contiguous"):
        score.check_scores(broken, CONFIG)


def test_run_reads_gold_and_writes_score_table(tmp_path):
    db = str(tmp_path / "w.duckdb")
    con = duckdb.connect(db)
    con.execute("create schema gold")
    older = metrics(FIVE).assign(month=MONTH - pd.DateOffset(months=1))
    con.register("m", pd.concat([older, metrics(FIVE)]))
    con.execute("create table gold.gold_zip_investment_metrics as select * from m")
    con.close()

    summary = score.run(db)
    assert summary["snapshot_month"] == "2026-09-01"
    assert summary["zips_scored"] == 5
    con = duckdb.connect(db, read_only=True)
    top = con.execute(
        "select zip, national_rank from gold.zip_opportunity_score "
        "order by national_rank limit 1"
    ).fetchall()
    con.close()
    assert top == [("00001", 1)]
