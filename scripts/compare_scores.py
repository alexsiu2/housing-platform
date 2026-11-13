"""
Before/after comparison of two scoring configs on the same snapshot month
(design doc §7.5: any weights change ships with a top-50 comparison).

Scores in memory only; gold.zip_opportunity_score is not touched.

    python scripts/compare_scores.py \\
        --before tests/fixtures/scoring_v1.0.yaml --after config/scoring.yaml
    python scripts/compare_scores.py --top 50 --markdown out.md
"""

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scoring.score import (  # noqa: E402
    DEFAULT_CONFIG,
    METRICS_TABLE,
    compute_scores,
    duckdb_path,
    load_config,
)


def score_month(db: str, month: str | None, config_path: str) -> pd.DataFrame:
    con = duckdb.connect(db, read_only=True)
    try:
        snapshot = pd.Timestamp(
            month or con.sql(f"select max(month) from {METRICS_TABLE}").fetchall()[0][0]
        )
        metrics = con.execute(
            f"select * from {METRICS_TABLE} where month = ?", [snapshot.date()]
        ).df()
    finally:
        con.close()
    metrics["month"] = pd.to_datetime(metrics["month"])
    return compute_scores(metrics, load_config(config_path), snapshot)


def compare(before: pd.DataFrame, after: pd.DataFrame, top: int) -> dict:
    b = before.set_index("zip")
    a = after.set_index("zip")
    top_b, top_a = set(b.index[b.national_rank <= top]), set(
        a.index[a.national_rank <= top]
    )
    both = b.index.intersection(a.index)
    moves = (b.loc[both, "national_rank"] - a.loc[both, "national_rank"]).abs()
    tiers = pd.crosstab(
        b.loc[both, "tier"],
        a.loc[both, "tier"],
        rownames=["before"],
        colnames=["after"],
    )
    return {
        "top": top,
        "kept": sorted(top_b & top_a),
        "dropped": sorted(top_b - top_a, key=lambda z: b.at[z, "national_rank"]),
        "added": sorted(top_a - top_b, key=lambda z: a.at[z, "national_rank"]),
        "partial_in_top_before": int(b.loc[list(top_b), "partial_score"].sum()),
        "partial_in_top_after": int(a.loc[list(top_a), "partial_score"].sum()),
        "rank_corr": round(
            b.loc[both, "national_rank"].corr(a.loc[both, "national_rank"], "spearman"),
            4,
        ),
        "median_rank_move": float(moves.median()),
        "tiers": tiers,
    }


def to_markdown(before, after, c, v_before, v_after) -> str:
    b, a = before.set_index("zip"), after.set_index("zip")

    def row(z, df):
        r = df.loc[z]
        return f"{z} {r.city}, {r.state}"

    lines = [
        f"### Top {c['top']}: {v_before} → {v_after}",
        "",
        f"- Kept in top {c['top']}: **{len(c['kept'])}**; dropped "
        f"{len(c['dropped'])}; added {len(c['added'])}",
        f"- Partial-score zips in top {c['top']}: "
        f"{c['partial_in_top_before']} → {c['partial_in_top_after']}",
        f"- Spearman rank correlation, all zips: {c['rank_corr']}; "
        f"median rank move {c['median_rank_move']:.0f}",
        "",
        f"| | zip | {v_before} rank | {v_after} rank | partial |",
        "|---|---|---|---|---|",
    ]
    for z in c["dropped"]:
        new_rank = a.at[z, "national_rank"] if z in a.index else "—"
        lines.append(
            f"| dropped | {row(z, b)} | {b.at[z, 'national_rank']} | {new_rank} | "
            f"{'yes' if b.at[z, 'partial_score'] else ''} |"
        )
    for z in c["added"]:
        old_rank = b.at[z, "national_rank"] if z in b.index else "—"
        lines.append(
            f"| added | {row(z, a)} | {old_rank} | {a.at[z, 'national_rank']} | "
            f"{'yes' if a.at[z, 'partial_score'] else ''} |"
        )
    lines += ["", "Tier changes (rows: before, columns: after):", "", "```",
              c["tiers"].to_string(), "```"]  # fmt: skip
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Compare two scoring configs")
    p.add_argument("--before", default="tests/fixtures/scoring_v1.0.yaml")
    p.add_argument("--after", default=str(DEFAULT_CONFIG))
    p.add_argument("--month", help="YYYY-MM-01 (default: latest)")
    p.add_argument("--db", default=None)
    p.add_argument("--top", type=int, default=50)
    p.add_argument("--markdown", help="write the report to this file")
    args = p.parse_args(argv)

    db = args.db or duckdb_path()
    before = score_month(db, args.month, args.before)
    after = score_month(db, args.month, args.after)
    c = compare(before, after, args.top)
    report = to_markdown(
        before,
        after,
        c,
        load_config(args.before).weights_version,
        load_config(args.after).weights_version,
    )
    if args.markdown:
        Path(args.markdown).write_text(report)
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
