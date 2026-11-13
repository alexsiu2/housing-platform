from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import boto3
import pandas as pd
import pytest
import requests
from deltalake import DeltaTable
from moto import mock_aws

from ingestion import zillow_client, zillow_pipeline
from ingestion.validation import DataQualityError

HEADER = (
    "RegionID,SizeRank,RegionName,RegionType,StateName,State,City,Metro,CountyName,"
    "2023-11-30,2023-12-31,2024-01-31"
)
# Second row has a 4-digit zip (leading zero lost) and a missing month.
CSV = (
    HEADER
    + "\n"
    + '91982,0,77494,zip,TX,TX,Katy,"Houston-The Woodlands-Sugar Land, TX",'
    + "Fort Bend County,1500.5,1510.0,1520.25\n"
    + '58196,5,1001,zip,MA,MA,Agawam,"Springfield, MA",Hampden County,,1300,1310\n'
).encode()

RUN_TS = datetime(2024, 2, 15, 9, 0, tzinfo=timezone.utc)


# ── parse ──────────────────────────────────────────────────────────────────


def test_parse_melts_to_long_and_drops_empty_months():
    df = zillow_client.parse_wide_csv(CSV, "zori")
    assert len(df) == 5  # 3 + 2 (one empty cell dropped)
    assert list(df.columns[:4]) == ["zip", "month", "year", "rent_index"]
    katy = df[df["zip"] == "77494"].sort_values("month")
    assert katy["rent_index"].tolist() == [1500.5, 1510.0, 1520.25]


def test_parse_zero_pads_zip_and_uses_month_start():
    df = zillow_client.parse_wide_csv(CSV, "zhvi")
    assert "01001" in set(df["zip"])
    assert set(df["month"]) == set(
        pd.to_datetime(["2023-11-01", "2023-12-01", "2024-01-01"])
    )
    assert set(df["year"]) == {2023, 2024}
    assert "home_value" in df.columns


def test_parse_since_month_keeps_trailing_window():
    df = zillow_client.parse_wide_csv(CSV, "zori", since_month="2024-01-01")
    assert set(df["month"]) == {pd.Timestamp("2024-01-01")}
    assert len(df) == 2


def test_parse_fails_loudly_on_schema_change():
    bad = CSV.replace(b"RegionName", b"ZipCode")
    with pytest.raises(zillow_client.SchemaError, match="RegionName"):
        zillow_client.parse_wide_csv(bad, "zori")


def test_parse_fails_without_month_columns():
    with pytest.raises(zillow_client.SchemaError, match="month columns"):
        zillow_client.parse_wide_csv(HEADER.rsplit(",", 3)[0].encode() + b"\n", "zori")


def test_unknown_dataset_rejected():
    with pytest.raises(ValueError, match="Unknown Zillow dataset"):
        zillow_client.dataset_url("zzz")


# ── download ───────────────────────────────────────────────────────────────


def _resp(status, content=b""):
    r = MagicMock(status_code=status, content=content)
    r.raise_for_status.side_effect = (
        requests.HTTPError(f"HTTP {status}") if status >= 400 else None
    )
    return r


def test_download_retries_then_succeeds():
    with patch.object(
        zillow_client.requests, "get", side_effect=[_resp(503), _resp(200, CSV)]
    ) as get:
        assert zillow_client.download_csv("zori") == CSV
    assert get.call_count == 2
    assert get.call_args.kwargs["timeout"] == zillow_client.REQUEST_TIMEOUT_SECONDS


def test_download_does_not_retry_404():
    with patch.object(zillow_client.requests, "get", return_value=_resp(404)) as get:
        with pytest.raises(requests.HTTPError):
            zillow_client.download_csv("zori")
    assert get.call_count == 1


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


def _land(s3, content=CSV, run_ts=RUN_TS):
    with patch.object(zillow_pipeline, "download_csv", return_value=content):
        return zillow_pipeline.extract_to_raw("zori", run_ts, client=s3)


def test_raw_key_layout():
    assert zillow_pipeline.raw_key("zori", RUN_TS) == (
        "zillow/zori/2024/02/Zip_zori_uc_sfrcondomfr_sm_month_15090000000000.csv"
    )


def test_since_month():
    assert zillow_pipeline.since_month(3, date(2024, 2, 15)) == "2023-11-01"
    assert zillow_pipeline.since_month(None) is None


def test_extract_lands_untouched_csv(s3):
    key = _land(s3)
    assert s3.get_object(Bucket="raw", Key=key)["Body"].read() == CSV


def test_full_load_then_windowed_reload_is_idempotent(s3, tmp_path):
    uri = str(tmp_path / "zori")
    key = _land(s3)
    m = zillow_pipeline.load_raw_to_bronze(
        "zori", key, lookback_months=None, client=s3, table_uri=uri, storage_options={}
    )
    assert m["rows_inserted"] == 5 and m["zips"] == 2
    assert DeltaTable(uri).metadata().partition_columns == ["year"]

    # Next month Zillow revises Jan 2024; reload only the trailing window.
    revised = CSV.replace(b"1520.25", b"1525.0")
    key2 = _land(s3, revised, run_ts=datetime(2024, 3, 15, tzinfo=timezone.utc))
    m2 = zillow_pipeline.load_raw_to_bronze(
        "zori",
        key2,
        lookback_months=1,
        today=date(2024, 2, 20),
        client=s3,
        table_uri=uri,
        storage_options={},
    )
    out = DeltaTable(uri).to_pandas()
    assert m2["since_month"] == "2024-01-01"
    assert m2["rows_updated"] == 2 and m2["rows_inserted"] == 0
    assert len(out) == 5
    jan = out[(out["zip"] == "77494") & (out["month"] == pd.Timestamp("2024-01-01"))]
    assert jan["rent_index"].item() == 1525.0
    assert out["city"].dtype == object  # categoricals stored as plain strings


def test_load_rejects_non_positive_values(s3, tmp_path):
    key = _land(s3, CSV.replace(b"1300", b"-5"))
    with pytest.raises(DataQualityError, match=r"to_be_between\(rent_index\)"):
        zillow_pipeline.load_raw_to_bronze(
            "zori",
            key,
            lookback_months=None,
            client=s3,
            table_uri=str(tmp_path / "z"),
            storage_options={},
        )
    assert not DeltaTable.is_deltatable(str(tmp_path / "z"))
