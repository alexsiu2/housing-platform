"""
Opportunity score (design doc §7): Gold metrics -> gold.zip_opportunity_score.

For the snapshot month (latest month in the Gold metrics by default):
  1. Eligibility: required metrics present (§7.4), population >= min.
  2. Each sub-score = percentile rank among eligible zips, 0-100
     ("80" = beats 80% of zips); lower-is-better metrics are inverted.
  3. Composite = weighted sum; weights of missing optional sub-scores are
     redistributed proportionally and the row is flagged partial_score.
  4. Tier, national_rank (contiguous 1..N), metro_rank.

Weights come from config/scoring.yaml and every run records weights_version.
Output is checked before writing; a failed check raises and writes nothing.

    python -m scoring.score                     # latest month
    python -m scoring.score --month 2026-06-01
"""

import argparse
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "config" / "scoring.yaml"
METRICS_TABLE = "gold.gold_zip_investment_metrics"
SCORE_TABLE = "gold.zip_opportunity_score"

CONTEXT_COLUMNS = [
    "cbsa_code", "metro", "state", "city", "rent_index", "home_value",
    "median_household_income", "population", "demographics_vintage",
    "job_growth_month",
]  # fmt: skip


class ScoreCheckError(RuntimeError):
    """The computed scores failed a sanity check; nothing was written."""


@dataclass(frozen=True)
class SubScore:
    name: str
    metric: str
    higher_is_better: bool
    weight: float


@dataclass(frozen=True)
class ScoringConfig:
    weights_version: str
    sub_scores: tuple[SubScore, ...]
    required: tuple[str, ...]
    strong: float
    moderate: float
    min_population: float
    max_job_lag_months: int


def load_config(path: Path | str = DEFAULT_CONFIG) -> ScoringConfig:
    raw = yaml.safe_load(Path(path).read_text())
    subs = []
    for name, spec in raw["sub_scores"].items():
        if spec["direction"] not in ("higher", "lower"):
            raise ValueError(f"{name}: direction must be 'higher' or 'lower'")
        subs.append(
            SubScore(
                name, spec["metric"], spec["direction"] == "higher", spec["weight"]
            )
        )
    total = sum(s.weight for s in subs)
    if abs(total - 1) > 1e-9:
        raise ValueError(f"Sub-score weights must sum to 1, got {total}")
    names = {s.name for s in subs}
    unknown = set(raw["required"]) - names
    if unknown:
        raise ValueError(f"required lists unknown sub-scores: {sorted(unknown)}")
    elig = raw.get("eligibility", {})
    return ScoringConfig(
        weights_version=str(raw["weights_version"]),
        sub_scores=tuple(subs),
        required=tuple(raw["required"]),
        strong=float(raw["tiers"]["strong"]),
        moderate=float(raw["tiers"]["moderate"]),
        min_population=float(elig.get("min_population", 0)),
        max_job_lag_months=int(elig.get("max_job_data_lag_months", 120)),
    )


def percentile_score(values: pd.Series, higher_is_better: bool) -> pd.Series:
    """
    0-100 percentile rank among non-null values: the share of other zips this
    one beats (ties share the lower rank). Nulls stay null.
    """
    present = values.dropna()
    n = len(present)
    out = pd.Series(float("nan"), index=values.index)
    if n == 0:
        return out
    if n == 1:
        out[present.index] = 100.0
        return out
    ranks = present.rank(method="min", ascending=higher_is_better)
    out[present.index] = (ranks - 1) / (n - 1) * 100
    return out


def compute_scores(
    metrics: pd.DataFrame, config: ScoringConfig, snapshot_month: pd.Timestamp
) -> pd.DataFrame:
    """Score one month of gold_zip_investment_metrics. Pure; no I/O."""
    df = metrics[metrics["month"] == snapshot_month].copy()
    if df.empty:
        raise ValueError(f"No metrics rows for {snapshot_month:%Y-%m}")
    subs = {s.name: s for s in config.sub_scores}

    # Stale job data counts as missing (§7.4 then redistributes its weight).
    if "yoy_job_growth_pct" in df and "job_growth_month" in df:
        job_month = pd.to_datetime(df["job_growth_month"])
        lag = (snapshot_month.year - job_month.dt.year) * 12 + (
            snapshot_month.month - job_month.dt.month
        )
        df.loc[lag > config.max_job_lag_months, "yoy_job_growth_pct"] = float("nan")

    required_metrics = [subs[n].metric for n in config.required]
    eligible = df[required_metrics].notna().all(axis=1)
    if config.min_population:
        eligible &= df["population"].fillna(0) >= config.min_population
    df = df[eligible].copy()
    if df.empty:
        raise ValueError(f"No eligible zips for {snapshot_month:%Y-%m}")

    for s in config.sub_scores:
        df[s.name] = percentile_score(df[s.metric], s.higher_is_better).round(2)

    weights = pd.DataFrame(
        {s.name: df[s.name].notna() * s.weight for s in config.sub_scores}
    )
    weight_present = weights.sum(axis=1)
    weighted = sum(df[s.name].fillna(0) * s.weight for s in config.sub_scores)
    df["opportunity_score"] = (weighted / weight_present).round(2)
    df["partial_score"] = weight_present < 1 - 1e-9

    df["tier"] = "weak"
    df.loc[df["opportunity_score"] >= config.moderate, "tier"] = "moderate"
    df.loc[df["opportunity_score"] >= config.strong, "tier"] = "strong"

    df = df.sort_values(["opportunity_score", "zip"], ascending=[False, True])
    df["national_rank"] = range(1, len(df) + 1)
    df["metro_rank"] = (
        df.groupby("cbsa_code", dropna=True)["opportunity_score"]
        .rank(method="first", ascending=False)
        .astype("Int64")
    )
    df["metro_zip_count"] = (
        df.groupby("cbsa_code", dropna=True)["zip"].transform("count").astype("Int64")
    )

    df["snapshot_month"] = snapshot_month
    df["weights_version"] = config.weights_version
    df["scored_at"] = datetime.now(timezone.utc)

    metric_cols = [s.metric for s in config.sub_scores]
    sub_cols = [s.name for s in config.sub_scores]
    columns = (
        ["zip", "snapshot_month", "national_rank", "opportunity_score", "tier"]
        + sub_cols
        + ["partial_score", "metro_rank", "metro_zip_count"]
        + metric_cols
        + [c for c in CONTEXT_COLUMNS if c in df.columns]
        + ["weights_version", "scored_at"]
    )
    return df[columns].reset_index(drop=True)


def check_scores(scores: pd.DataFrame, config: ScoringConfig) -> None:
    """Sanity checks before writing (design doc §10.1). Raises on failure."""
    problems = []
    sub_cols = [s.name for s in config.sub_scores]
    for col in ["opportunity_score", *sub_cols]:
        bad = scores[col].dropna()
        if ((bad < 0) | (bad > 100)).any():
            problems.append(f"{col} outside 0-100")
    if scores["zip"].duplicated().any():
        problems.append("duplicate zips")
    if sorted(scores["national_rank"]) != list(range(1, len(scores) + 1)):
        problems.append("national_rank not contiguous 1..N")
    if scores[[s for s in config.required]].isna().any().any():
        problems.append("required sub-score is null")
    if not set(scores["tier"]) <= {"strong", "moderate", "weak"}:
        problems.append("unknown tier")
    if problems:
        raise ScoreCheckError("; ".join(problems))


def duckdb_path() -> str:
    default = REPO_ROOT / "data" / "rpof.duckdb"
    path = os.getenv("DUCKDB_PATH")
    if not path:
        return str(default)
    # .env uses a path relative to dbt/ (e.g. ../data/rpof.duckdb).
    p = Path(path)
    return str(p if p.is_absolute() else (REPO_ROOT / "dbt" / p).resolve())


def run(
    db_path: str | None = None,
    month: str | None = None,
    config_path: Path | str = DEFAULT_CONFIG,
) -> dict:
    """Score the snapshot month and replace gold.zip_opportunity_score."""
    config = load_config(config_path)
    con = duckdb.connect(db_path or duckdb_path())
    try:
        snapshot = pd.Timestamp(
            month or con.sql(f"select max(month) from {METRICS_TABLE}").fetchall()[0][0]
        )
        metrics = con.execute(
            f"select * from {METRICS_TABLE} where month = ?", [snapshot.date()]
        ).df()
        metrics["month"] = pd.to_datetime(metrics["month"])
        scores = compute_scores(metrics, config, snapshot)
        check_scores(scores, config)

        con.register("scores_df", scores)
        con.execute(f"create or replace table {SCORE_TABLE} as select * from scores_df")
        con.unregister("scores_df")
    finally:
        con.close()

    summary = {
        "snapshot_month": snapshot.date().isoformat(),
        "weights_version": config.weights_version,
        "zips_in_month": int(len(metrics)),
        "zips_scored": int(len(scores)),
        "partial_scores": int(scores["partial_score"].sum()),
        "tiers": {k: int(v) for k, v in scores["tier"].value_counts().items()},
    }
    logger.info("Scored: %s", summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compute zip opportunity scores")
    parser.add_argument("--month", help="Snapshot month YYYY-MM-01 (default: latest)")
    parser.add_argument("--db", help="DuckDB path (default: DUCKDB_PATH or data/)")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    print(run(args.db, args.month, args.config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
