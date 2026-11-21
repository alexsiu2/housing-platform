"""
GDELT housing news via BigQuery (design doc D15): the GKG table is 22 TB, so
the filter runs where it lives and only matching US housing articles come back.

Auth is keyless Application Default Credentials: locally, `gcloud auth
application-default login --impersonate-service-account=<reader SA>`; queries
run as that least-privilege service account (BigQuery Job User only).

Every query is dry-run first; if it would scan more than the budget it is
refused before any cost. Jobs also set maximum_bytes_billed as a second guard.
The BigQuery sandbox has no billing, so overruns fail rather than charge.
"""

import logging
import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

from ingestion.gdelt_themes import HOUSING_THEMES

load_dotenv()

logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).with_name("sql") / "gdelt_housing_articles.sql"

# The housing theme allowlist lives in gdelt_themes (shared with the live producer).

DEFAULT_MAX_GB_PER_QUERY = 60.0  # ~7 months of news; a month is ~9 GB


class QueryBudgetExceeded(RuntimeError):
    """The dry run estimated more bytes than this run is allowed to scan."""


def project() -> str:
    p = os.getenv("GCP_PROJECT")
    if not p:
        raise ValueError("GCP_PROJECT is not set. Add it to your .env file.")
    return p


def get_client():
    from google.cloud import bigquery

    return bigquery.Client(project=project())


def _params(start: date, end: date):
    from google.cloud import bigquery

    return [
        bigquery.ArrayQueryParameter("housing_themes", "STRING", HOUSING_THEMES),
        bigquery.ScalarQueryParameter("start_date", "DATE", start),
        bigquery.ScalarQueryParameter("end_date", "DATE", end),
    ]


def estimate_gb(start: date, end: date, client=None) -> float:
    """Dry run: GB the query would scan. Free."""
    from google.cloud import bigquery

    client = client or get_client()
    cfg = bigquery.QueryJobConfig(
        dry_run=True, use_query_cache=False, query_parameters=_params(start, end)
    )
    job = client.query(SQL_PATH.read_text(), job_config=cfg)
    return job.total_bytes_processed / 1e9


def fetch_articles(
    start: date,
    end: date,
    max_gb: float = DEFAULT_MAX_GB_PER_QUERY,
    client=None,
) -> tuple[list[dict], dict]:
    """
    Housing articles published in [start, end). Returns (rows, job_stats).

    Raises QueryBudgetExceeded (before running anything) if the dry run says
    the query would scan more than max_gb.
    """
    from google.cloud import bigquery

    if start >= end:
        raise ValueError(f"start {start} must be before end {end}")
    client = client or get_client()
    estimated = estimate_gb(start, end, client=client)
    if estimated > max_gb:
        raise QueryBudgetExceeded(
            f"GDELT query for {start}..{end} would scan {estimated:.1f} GB "
            f"(budget {max_gb:.0f} GB); use a smaller window"
        )
    cfg = bigquery.QueryJobConfig(
        query_parameters=_params(start, end),
        maximum_bytes_billed=int(max_gb * 1e9),
    )
    job = client.query(SQL_PATH.read_text(), job_config=cfg)
    rows = [dict(r) for r in job.result()]
    stats = {
        "job_id": job.job_id,
        "estimated_gb": round(estimated, 3),
        "billed_gb": round((job.total_bytes_billed or 0) / 1e9, 3),
        "cache_hit": bool(job.cache_hit),
        "rows": len(rows),
    }
    logger.info("GDELT %s..%s: %s", start, end, stats)
    return rows, stats
