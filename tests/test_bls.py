import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import boto3
import pandas as pd
import pytest
import requests
from deltalake import DeltaTable
from moto import mock_aws

from ingestion import bls_client, bls_pipeline
from ingestion.validation import DataQualityError

KEY = "bls-test-key-should-never-appear"
RUN_TS = datetime(2026, 10, 5, 8, 0, tzinfo=timezone.utc)
HOUSTON = "SMU48264200000000001"


def _payload(series=None):
    series = series or {
        HOUSTON: [
            ("2026", "M09", "3489.4", [{"code": "P", "text": "Preliminary"}]),
            ("2026", "M08", "3480.1", [{}]),
            ("2025", "M13", "3400.0", []),  # annual average: dropped
            ("2025", "M09", "3450.0", []),
        ],
        "SMU34356200000000001": [],  # candidate BLS doesn't publish: no data
    }
    return {
        "status": "REQUEST_SUCCEEDED",
        "message": [],
        "Results": {
            "series": [
                {
                    "seriesID": sid,
                    "data": [
                        {"year": y, "period": p, "value": v, "footnotes": f}
                        for y, p, v, f in points
                    ],
                }
                for sid, points in series.items()
            ]
        },
    }


@pytest.fixture
def key(monkeypatch):
    monkeypatch.setenv("BLS_API_KEY", KEY)


# ── client ─────────────────────────────────────────────────────────────────


def test_series_id_layout():
    assert bls_client.ces_series_id("48", "26420") == HOUSTON
    assert bls_client.ces_series_id("6", "41860") == "SMU06418600000000001"


def test_year_chunks_respect_20_year_limit():
    assert bls_client.year_chunks(2000, 2026) == [(2000, 2019), (2020, 2026)]
    assert bls_client.year_chunks(2024, 2024) == [(2024, 2024)]
    with pytest.raises(ValueError):
        bls_client.year_chunks(2026, 2020)


def test_parse_drops_annual_and_flags_preliminary():
    df = bls_client.parse_payload(_payload())
    assert len(df) == 3
    assert set(df["series_id"]) == {HOUSTON}
    latest = df[df["month"] == pd.Timestamp("2026-09-01")].iloc[0]
    assert latest["employment_thousands"] == 3489.4
    assert bool(latest["is_preliminary"]) is True
    assert (latest["state_fips"], latest["area_code"]) == ("48", "26420")


def test_parse_empty_response_has_columns():
    df = bls_client.parse_payload({"Results": {"series": []}})
    assert df.empty and list(df.columns) == bls_client.OUTPUT_COLUMNS


def test_fetch_batches_by_50_series_and_20_years(key):
    ok = MagicMock(status_code=200, ok=True)
    ok.json.return_value = _payload()
    ids = [f"SMU48{i:05d}0000000001" for i in range(120)]
    with patch.object(bls_client.requests, "post", return_value=ok) as post:
        payloads = bls_client.fetch_series(ids, 2000, 2026)
    assert len(payloads) == post.call_count == 6  # 3 batches x 2 windows
    sizes = [len(c.kwargs["json"]["seriesid"]) for c in post.call_args_list]
    assert sizes == [50, 50, 20, 50, 50, 20]
    assert post.call_args.kwargs["json"]["registrationkey"] == KEY


def test_bls_failure_status_raises_without_key(key):
    bad = MagicMock(status_code=200, ok=True)
    bad.json.return_value = {
        "status": "REQUEST_NOT_PROCESSED",
        "message": [f"Invalid key {KEY}", "Daily threshold reached"],
    }
    with patch.object(bls_client.requests, "post", return_value=bad):
        with pytest.raises(bls_client.BLSError) as exc:
            bls_client.fetch_batch([HOUSTON], 2025, 2026)
    assert KEY not in str(exc.value)
    assert "Daily threshold" in str(exc.value)


def test_retries_5xx_then_succeeds(key):
    ok = MagicMock(status_code=200, ok=True)
    ok.json.return_value = _payload()
    with patch.object(
        bls_client.requests,
        "post",
        side_effect=[MagicMock(status_code=503, ok=False), ok],
    ) as post:
        bls_client.fetch_batch([HOUSTON], 2025, 2026)
    assert post.call_count == 2


def test_missing_key_is_clear(monkeypatch):
    monkeypatch.delenv("BLS_API_KEY", raising=False)
    with pytest.raises(ValueError, match="BLS_API_KEY"):
        bls_client.fetch_batch([HOUSTON], 2025, 2026)


def test_connection_errors_exhaust_without_key(key):
    with patch.object(
        bls_client.requests, "post", side_effect=requests.Timeout(KEY)
    ) as post:
        with pytest.raises(requests.ConnectionError) as exc:
            bls_client.fetch_batch([HOUSTON], 2025, 2026)
    assert post.call_count == bls_client.MAX_ATTEMPTS
    assert KEY not in str(exc.value)


# ── candidates ─────────────────────────────────────────────────────────────


def test_candidates_one_per_metro_state_latest_vintage():
    xwalk = pd.DataFrame(
        {
            "crosswalk_type": ["zip-cbsa"] * 5 + ["tract-zip"],
            "vintage": ["2026Q2"] * 4 + ["2025Q1", "2026Q2"],
            "geoid": ["35620", "35620", "35620", "99999", "11111", "36061000100"],
            "state": ["NY", "NJ", "NY", "MT", "TX", "NY"],
        }
    )
    assert bls_pipeline.candidate_series(xwalk) == [
        "SMU34356200000000001",
        "SMU36356200000000001",
    ]


# ── pipeline ───────────────────────────────────────────────────────────────


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("STORAGE_ACCESS_KEY", "test")
    monkeypatch.setenv("STORAGE_SECRET_KEY", "test")
    monkeypatch.delenv("STORAGE_ENDPOINT", raising=False)
    monkeypatch.setenv("RAW_BUCKET", "raw")
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="raw")
        yield client


def _land(s3, payloads, run_ts=RUN_TS):
    with patch.object(bls_pipeline, "fetch_series", return_value=payloads):
        return bls_pipeline.extract_to_raw(
            2025, 2026, series_ids=[HOUSTON], run_ts=run_ts, client=s3
        )


def test_land_and_load_is_idempotent(s3, tmp_path):
    uri = str(tmp_path / "bls")
    keys = _land(s3, [_payload()])
    assert keys == ["bls/ces/2026/10/batch_000_05080000000000.json"]
    env = json.loads(s3.get_object(Bucket="raw", Key=keys[0])["Body"].read())
    assert env["response"]["status"] == "REQUEST_SUCCEEDED"

    m1 = bls_pipeline.load_raw_to_bronze(
        keys, client=s3, table_uri=uri, storage_options={}
    )
    m2 = bls_pipeline.load_raw_to_bronze(
        keys, client=s3, table_uri=uri, storage_options={}
    )
    assert m1["rows_inserted"] == 3 and m1["series_with_data"] == 1
    assert m2["rows_inserted"] == 0 and m2["rows_updated"] == 3
    assert len(DeltaTable(uri).to_pandas()) == 3


def test_load_rejects_nonpositive_employment(s3, tmp_path):
    keys = _land(s3, [_payload({HOUSTON: [("2026", "M01", "0", [])]})])
    with pytest.raises(DataQualityError, match="employment_thousands"):
        bls_pipeline.load_raw_to_bronze(
            keys, client=s3, table_uri=str(tmp_path / "b"), storage_options={}
        )


def test_load_with_no_data_at_all_fails(s3, tmp_path):
    keys = _land(s3, [_payload({HOUSTON: []})])
    with pytest.raises(RuntimeError, match="no data"):
        bls_pipeline.load_raw_to_bronze(
            keys, client=s3, table_uri=str(tmp_path / "b"), storage_options={}
        )
