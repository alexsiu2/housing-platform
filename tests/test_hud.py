import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import boto3
import pytest
import requests
from deltalake import DeltaTable
from moto import mock_aws

from ingestion import hud_client, hud_pipeline
from ingestion.validation import DataQualityError

TOKEN = "hud-test-token-should-never-appear"
RUN_TS = datetime(2025, 7, 1, 12, 0, tzinfo=timezone.utc)


def _zip_cbsa_payload(n=1200):
    results = [
        {
            "zip": f"{10000 + i}",
            "geoid": "35620",
            "res_ratio": 1.0,
            "bus_ratio": 1.0,
            "oth_ratio": 0.0,
            "tot_ratio": 1.0,
            "city": "NEW YORK",
            "state": "NY",
        }
        for i in range(n)
    ]
    # One zip split across two metros
    results.append({**results[0], "geoid": "14460", "res_ratio": 0.0})
    return {
        "data": {
            "year": "2025",
            "quarter": "2",
            "input": "All",
            "crosswalk_type": "zip-cbsa",
            "results": results,
        }
    }


def _tract_zip_payload(field="tract", n=1200):
    return {
        "data": {
            "year": 2025,
            "quarter": 2,
            "results": [
                {
                    field: f"{36061000100 + i}",
                    "zip": "1001" if i == 0 else f"{10000 + i}",
                    "res_ratio": 0.5,
                    "bus_ratio": 0.25,
                    "oth_ratio": 0,
                    "tot_ratio": 0.5,
                }
                for i in range(n)
            ],
        }
    }


@pytest.fixture
def token(monkeypatch):
    monkeypatch.setenv("HUD_API_TOKEN", TOKEN)


# ── client ─────────────────────────────────────────────────────────────────


def test_parse_zip_cbsa():
    df = hud_client.parse_crosswalk(_zip_cbsa_payload(), "zip-cbsa")
    assert list(df.columns) == hud_client.OUTPUT_COLUMNS
    assert set(df["vintage"]) == {"2025Q2"}
    assert (df["zip"] == "10000").sum() == 2


@pytest.mark.parametrize("field", ["tract", "geoid"])
def test_parse_tract_zip_accepts_either_field_name(field):
    df = hud_client.parse_crosswalk(_tract_zip_payload(field), "tract-zip")
    assert df["geoid"].iloc[0] == "36061000100"
    assert df["zip"].iloc[0] == "01001"  # zero-padded
    assert df["city"].isna().all()


def test_parse_rejects_empty_results():
    with pytest.raises(ValueError, match="No results"):
        hud_client.parse_crosswalk({"data": {"results": []}}, "zip-cbsa")


def test_missing_token_is_a_clear_error(monkeypatch):
    monkeypatch.delenv("HUD_API_TOKEN", raising=False)
    with pytest.raises(ValueError, match="HUD_API_TOKEN"):
        hud_client.fetch_crosswalk_json("zip-cbsa")


def test_fetch_sends_bearer_and_query_all(token):
    resp = MagicMock(status_code=200, ok=True)
    resp.json.return_value = {"data": {}}
    with patch.object(hud_client.requests, "get", return_value=resp) as get:
        hud_client.fetch_crosswalk_json("tract-zip", 2025, 2)
    kw = get.call_args.kwargs
    assert kw["headers"]["Authorization"] == f"Bearer {TOKEN}"
    assert kw["params"] == {"type": 6, "query": "All", "year": 2025, "quarter": 2}


@pytest.mark.parametrize("status", [401, 404])
def test_http_error_never_contains_token(token, status):
    resp = MagicMock(status_code=status, ok=False, text=f"bad token {TOKEN}")
    with patch.object(hud_client.requests, "get", return_value=resp):
        with pytest.raises(requests.HTTPError) as exc:
            hud_client.fetch_crosswalk_json("zip-cbsa")
    assert TOKEN not in str(exc.value)


def test_retries_then_gives_up_without_token_in_error(token):
    with patch.object(
        hud_client.requests, "get", side_effect=requests.Timeout(f"t {TOKEN}")
    ) as get:
        with pytest.raises(requests.ConnectionError) as exc:
            hud_client.fetch_crosswalk_json("zip-cbsa")
    assert get.call_count == hud_client.MAX_ATTEMPTS
    assert TOKEN not in str(exc.value)


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


def _land(s3, payload, crosswalk="zip-cbsa"):
    with patch.object(hud_pipeline, "fetch_crosswalk_json", return_value=payload):
        return hud_pipeline.extract_to_raw(crosswalk, run_ts=RUN_TS, client=s3)


def test_extract_keys_by_returned_vintage(s3):
    key = _land(s3, _zip_cbsa_payload())
    assert key == "hud/crosswalk/2025_Q2/zip-cbsa_20250701T120000000000.json"
    env = json.loads(s3.get_object(Bucket="raw", Key=key)["Body"].read())
    assert env["crosswalk"] == "zip-cbsa"


def test_load_both_crosswalks_and_reload_idempotent(s3, tmp_path):
    uri = str(tmp_path / "hud")
    k1 = _land(s3, _zip_cbsa_payload())
    k2 = _land(s3, _tract_zip_payload(), crosswalk="tract-zip")
    m1 = hud_pipeline.load_raw_to_bronze(
        k1, client=s3, table_uri=uri, storage_options={}
    )
    hud_pipeline.load_raw_to_bronze(k2, client=s3, table_uri=uri, storage_options={})
    m3 = hud_pipeline.load_raw_to_bronze(
        k1, client=s3, table_uri=uri, storage_options={}
    )

    out = DeltaTable(uri).to_pandas()
    assert m1["rows_inserted"] == 1201 and m1["vintage"] == "2025Q2"
    assert m3["rows_inserted"] == 0 and m3["rows_updated"] == 1201
    assert out.groupby("crosswalk_type").size().to_dict() == {
        "tract-zip": 1200,
        "zip-cbsa": 1201,
    }


def test_load_rejects_ratio_out_of_range(s3, tmp_path):
    payload = _zip_cbsa_payload()
    payload["data"]["results"][3]["res_ratio"] = 1.7
    key = _land(s3, payload)
    with pytest.raises(DataQualityError, match=r"to_be_between\(res_ratio\)"):
        hud_pipeline.load_raw_to_bronze(
            key, client=s3, table_uri=str(tmp_path / "h"), storage_options={}
        )
