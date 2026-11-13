import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import boto3
import pytest
import requests
from deltalake import DeltaTable
from moto import mock_aws

from ingestion import census_client, census_pipeline
from ingestion.validation import DataQualityError

KEY = "c" * 40
RUN_TS = datetime(2026, 1, 15, 6, 0, tzinfo=timezone.utc)
HEADER = ["NAME", *census_client.ACS_VARIABLES, "zip code tabulation area"]


def _rows(n=20000, bad_value=None):
    rows = [HEADER]
    for i in range(n):
        values = ["75000", "12000", "5000", "2000", "300", "1500"]
        if i == 1:
            values[0] = "-666666666"  # suppressed median income
        if bad_value is not None and i == 2:
            values[1] = bad_value
        rows.append([f"ZCTA5 {i:05d}", *values, f"{i:05d}"])
    return rows


def _resp(text, status=200, json_body=None):
    r = MagicMock(status_code=status, ok=status < 400, text=text)
    r.json.return_value = json_body
    return r


@pytest.fixture
def key(monkeypatch):
    monkeypatch.setenv("CENSUS_API_KEY", KEY)


# ── client ─────────────────────────────────────────────────────────────────


def test_parse_long_format_and_sentinels():
    df = census_client.parse_acs(_rows(3), 2024)
    assert len(df) == 3 * 6
    assert list(df.columns) == census_client.OUTPUT_COLUMNS
    income = df[(df.zcta == "00001") & (df.variable == "B19013_001E")].iloc[0]
    assert bool(income["is_missing"]) and income["value"] != income["value"]  # NaN
    pop = df[(df.zcta == "00000") & (df.variable_name == "population")].iloc[0]
    assert pop["value"] == 12000 and not pop["is_missing"]
    assert set(df["vintage_year"]) == {2024}


def test_parse_rejects_missing_columns():
    with pytest.raises(ValueError, match="missing columns"):
        census_client.parse_acs(
            [["NAME", "zip code tabulation area"], ["x", "1"]], 2024
        )


def test_invalid_key_html_with_200_is_an_error(key):
    html = "<html><head><title>Invalid Key</title></head></html>"
    with patch.object(census_client.requests, "get", return_value=_resp(html)):
        with pytest.raises(census_client.CensusKeyError, match="Invalid Key") as exc:
            census_client.fetch_acs_json(2024)
    assert KEY not in str(exc.value)


def test_missing_key_env(monkeypatch):
    monkeypatch.delenv("CENSUS_API_KEY", raising=False)
    with pytest.raises(census_client.CensusKeyError, match="CENSUS_API_KEY"):
        census_client.fetch_acs_json(2024)


def test_fetch_requests_all_zctas_and_variables(key):
    with patch.object(
        census_client.requests, "get", return_value=_resp("[[]]", json_body=_rows(1))
    ) as get:
        census_client.fetch_acs_json(2024)
    params = get.call_args.kwargs["params"]
    assert params["for"] == "zip code tabulation area:*"
    assert params["get"].split(",")[1:] == list(census_client.ACS_VARIABLES)
    assert get.call_args.args[0] == "https://api.census.gov/data/2024/acs/acs5"


def test_http_error_message_has_no_key(key):
    with patch.object(census_client.requests, "get", return_value=_resp("x", 404)):
        with pytest.raises(requests.HTTPError) as exc:
            census_client.fetch_acs_json(2024)
    assert KEY not in str(exc.value)


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


def _land(s3, rows):
    with patch.object(census_pipeline, "fetch_acs_json", return_value=rows):
        return census_pipeline.extract_to_raw(2024, RUN_TS, client=s3)


def test_land_load_idempotent(s3, tmp_path):
    uri = str(tmp_path / "acs")
    key = _land(s3, _rows())
    assert key == "census/acs5/2024/zcta_20260115T060000000000.json"
    assert (
        json.loads(s3.get_object(Bucket="raw", Key=key)["Body"].read())["vintage_year"]
        == 2024
    )
    m1 = census_pipeline.load_raw_to_bronze(
        key, client=s3, table_uri=uri, storage_options={}
    )
    m2 = census_pipeline.load_raw_to_bronze(
        key, client=s3, table_uri=uri, storage_options={}
    )
    assert m1["rows_inserted"] == 120000 and m1["zctas"] == 20000
    assert m2["rows_inserted"] == 0
    assert DeltaTable(uri).metadata().partition_columns == ["vintage_year"]


def test_partial_download_rejected(s3, tmp_path):
    key = _land(s3, _rows(n=500))
    with pytest.raises(DataQualityError, match="row_count"):
        census_pipeline.load_raw_to_bronze(
            key, client=s3, table_uri=str(tmp_path / "a"), storage_options={}
        )
