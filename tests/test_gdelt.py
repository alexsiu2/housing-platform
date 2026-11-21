import json
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import boto3
import pytest
from deltalake import DeltaTable
from moto import mock_aws

from ingestion import gdelt_client, gdelt_pipeline
from ingestion.validation import DataQualityError


def _row(i, ts=20261110120000, tone=-1.5):
    return {
        "article_id": f"{ts}-{i}",
        "gdelt_timestamp": ts,
        "url": f"https://news.example/{i}",
        "source": "News.Example",
        "tone": tone,
        "housing_themes": ["ECON_HOUSING_PRICES"],
        "zctas": ["77002", "1001"],
        "places": ["Houston, Texas, United States"],
    }


def _fake_client(gb=2.5, rows=None):
    """A BigQuery client whose dry runs report `gb` and whose jobs return rows."""
    client = MagicMock()

    def query(sql, job_config):
        job = MagicMock()
        job.total_bytes_processed = int(gb * 1e9)
        job.total_bytes_billed = int(gb * 1e9)
        job.cache_hit = False
        job.job_id = "job-1"
        job.result.return_value = rows or []
        return job

    client.query.side_effect = query
    return client


@pytest.fixture(autouse=True)
def project(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "test-project")


# ── windows ────────────────────────────────────────────────────────────────


def test_windows_are_monday_aligned_and_capped():
    w = gdelt_pipeline.windows(date(2026, 1, 7), date(2026, 3, 5), window_days=28)
    assert w[0] == (date(2026, 1, 5), date(2026, 2, 2))  # rounded down to Monday
    assert all(s.weekday() == 0 and e.weekday() == 0 for s, e in w)
    assert all((e - s).days <= 28 for s, e in w)
    assert w[-1][1] == date(2026, 3, 9)  # end rounded up to the next Monday


def test_window_days_must_be_whole_weeks():
    with pytest.raises(ValueError, match="multiple of 7"):
        gdelt_pipeline.windows(date(2026, 1, 5), date(2026, 2, 5), window_days=30)


def test_last_complete_week():
    assert gdelt_pipeline.last_complete_week(date(2026, 11, 20)) == (
        date(2026, 11, 9),
        date(2026, 11, 16),
    )


# ── client guard ───────────────────────────────────────────────────────────


def test_budget_guard_refuses_before_running():
    client = _fake_client(gb=300)
    with pytest.raises(gdelt_client.QueryBudgetExceeded, match="300.0 GB"):
        gdelt_client.fetch_articles(date(2024, 1, 1), date(2026, 11, 16), 10, client)
    assert client.query.call_count == 1  # only the dry run
    assert client.query.call_args.kwargs["job_config"].dry_run is True


def test_fetch_sets_bytes_billed_cap_and_returns_stats():
    client = _fake_client(gb=2.88, rows=[_row(1)])
    rows, stats = gdelt_client.fetch_articles(
        date(2026, 11, 9), date(2026, 11, 16), 10, client
    )
    real = client.query.call_args_list[1].kwargs["job_config"]
    assert real.maximum_bytes_billed == int(10e9)
    assert stats["estimated_gb"] == 2.88 and stats["rows"] == 1
    assert len(rows) == 1


def test_inverted_window_rejected():
    with pytest.raises(ValueError, match="before"):
        gdelt_client.fetch_articles(
            date(2026, 2, 1), date(2026, 1, 1), 10, _fake_client()
        )


# ── pipeline ───────────────────────────────────────────────────────────────


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("STORAGE_ACCESS_KEY", "test")
    monkeypatch.setenv("STORAGE_SECRET_KEY", "test")
    monkeypatch.delenv("STORAGE_ENDPOINT", raising=False)
    monkeypatch.setenv("RAW_BUCKET", "raw")
    with mock_aws():
        c = boto3.client("s3", region_name="us-east-1")
        c.create_bucket(Bucket="raw")
        yield c


def test_parse_rows_types_and_zero_pads_zips():
    df = gdelt_pipeline.parse_rows([_row(1)])
    r = df.iloc[0]
    assert r.published_date == date(2026, 11, 10) and r.source == "news.example"
    assert r.zctas == ["77002", "01001"]


def test_land_and_load_idempotent(s3, tmp_path):
    client = _fake_client(gb=2.5, rows=[_row(i) for i in range(50)])
    run_ts = datetime(2026, 11, 20, tzinfo=timezone.utc)
    keys = gdelt_pipeline.extract_to_raw(
        date(2026, 11, 9), date(2026, 11, 16), client=client, s3=s3, run_ts=run_ts
    )
    assert len(keys) == 1 and keys[0].startswith("gdelt/housing_articles/2026/11/")
    env = json.loads(s3.get_object(Bucket="raw", Key=keys[0])["Body"].read())
    assert env["job"]["billed_gb"] == 2.5 and len(env["rows"]) == 50

    uri = str(tmp_path / "g")
    m1 = gdelt_pipeline.load_raw_to_bronze(
        keys, s3=s3, table_uri=uri, storage_options={}
    )
    m2 = gdelt_pipeline.load_raw_to_bronze(
        keys, s3=s3, table_uri=uri, storage_options={}
    )
    assert m1["rows_inserted"] == 50 and m1["bigquery_billed_gb"] == 2.5
    assert m2["rows_inserted"] == 0
    out = DeltaTable(uri).to_pandas()
    assert len(out) == 50 and list(out["zctas"].iloc[0]) == ["77002", "01001"]


def test_load_rejects_impossible_tone(s3, tmp_path):
    client = _fake_client(rows=[_row(1, tone=250.0)])
    keys = gdelt_pipeline.extract_to_raw(
        date(2026, 11, 9), date(2026, 11, 16), client=client, s3=s3
    )
    with pytest.raises(DataQualityError, match="tone"):
        gdelt_pipeline.load_raw_to_bronze(
            keys, s3=s3, table_uri=str(tmp_path / "g"), storage_options={}
        )


def test_reused_gdelt_id_for_different_articles_is_kept(s3, tmp_path):
    """GDELT reused 20240228201500-T161 for two different articles."""
    a, b = _row(1), _row(1)
    b["url"] = "https://other.example/x"
    keys = gdelt_pipeline.extract_to_raw(
        date(2026, 11, 9), date(2026, 11, 16), client=_fake_client(rows=[a, b]), s3=s3
    )
    m = gdelt_pipeline.load_raw_to_bronze(
        keys, s3=s3, table_uri=str(tmp_path / "g"), storage_options={}
    )
    assert m["rows_inserted"] == 2
