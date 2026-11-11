import json
from datetime import date, datetime, timezone
from unittest.mock import patch

import boto3
import pandas as pd
import pytest
from deltalake import DeltaTable
from moto import mock_aws

from ingestion import fred_pipeline
from ingestion.validation import DataQualityError, validate_fred_observations

RUN_TS = datetime(2024, 6, 3, 6, 0, tzinfo=timezone.utc)


def payload(*obs):
    return {"observations": [{"date": d, "value": v} for d, v in obs]}


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


def fake_fetch(responses):
    def _fetch(series_id, start, end):
        r = responses[series_id]
        if isinstance(r, Exception):
            raise r
        return r

    return _fetch


# ── extract ────────────────────────────────────────────────────────────────


def test_extract_lands_one_envelope_per_series(s3):
    responses = {
        "UNRATE": payload(("2024-01-01", "3.7")),
        "HOUST": payload(("2024-01-01", "1400")),
    }
    with patch.object(fred_pipeline, "fetch_series_json", fake_fetch(responses)):
        keys = fred_pipeline.extract_to_raw(
            "2024-01-01", "2024-02-01", ["UNRATE", "HOUST"], RUN_TS, client=s3
        )

    assert keys == [
        "fred/2024/06/03/UNRATE_060000000000.json",
        "fred/2024/06/03/HOUST_060000000000.json",
    ]
    env = json.loads(s3.get_object(Bucket="raw", Key=keys[0])["Body"].read())
    assert env["series_id"] == "UNRATE"
    assert env["observation_start"] == "2024-01-01"
    assert env["response"] == responses["UNRATE"]


def test_extract_skips_failed_series(s3):
    responses = {"UNRATE": payload(("2024-01-01", "3.7")), "HOUST": RuntimeError("x")}
    with patch.object(fred_pipeline, "fetch_series_json", fake_fetch(responses)):
        keys = fred_pipeline.extract_to_raw(
            "2024-01-01", "2024-02-01", ["UNRATE", "HOUST"], RUN_TS, client=s3
        )
    assert len(keys) == 1


def test_extract_raises_if_all_fail(s3):
    responses = {"UNRATE": RuntimeError("x")}
    with patch.object(fred_pipeline, "fetch_series_json", fake_fetch(responses)):
        with pytest.raises(RuntimeError, match="Failed to extract any"):
            fred_pipeline.extract_to_raw(
                "2024-01-01", "2024-02-01", ["UNRATE"], RUN_TS, client=s3
            )


# ── window ─────────────────────────────────────────────────────────────────


def test_window_defaults_to_lookback_from_today():
    assert fred_pipeline.resolve_window(lookback_days=10, today=date(2024, 3, 5)) == (
        "2024-02-24",
        "2024-03-05",
    )


def test_window_explicit_dates_win():
    assert fred_pipeline.resolve_window(
        "2019-01-01", "2020-06-30", lookback_days=10, today=date(2024, 3, 5)
    ) == ("2019-01-01", "2020-06-30")


def test_window_start_only_runs_to_today():
    assert fred_pipeline.resolve_window("2023-01-01", today=date(2024, 3, 5)) == (
        "2023-01-01",
        "2024-03-05",
    )


def test_window_rejects_inverted_range():
    with pytest.raises(ValueError, match="after end_date"):
        fred_pipeline.resolve_window("2024-02-01", "2024-01-01")


# ── load ───────────────────────────────────────────────────────────────────


def _extract(s3, responses, run_ts=RUN_TS):
    with patch.object(fred_pipeline, "fetch_series_json", fake_fetch(responses)):
        return fred_pipeline.extract_to_raw(
            "2024-01-01", "2024-03-01", list(responses), run_ts, client=s3
        )


def test_load_writes_bronze_with_lineage(s3, tmp_path):
    keys = _extract(
        s3,
        {
            "UNRATE": payload(("2024-01-01", "3.7"), ("2024-02-01", ".")),
            "HOUST": payload(("2024-01-01", "1400")),
        },
    )
    uri = str(tmp_path / "bronze")
    m = fred_pipeline.load_raw_to_bronze(
        keys, client=s3, table_uri=uri, storage_options={}
    )

    out = DeltaTable(uri).to_pandas()
    assert m["rows_inserted"] == 2  # "." row dropped
    assert m["validation"]["success"] is True
    assert set(out["source_file"]) == {f"s3://raw/{k}" for k in keys}
    assert DeltaTable(uri).metadata().partition_columns == ["series_id"]


def test_reloading_with_revised_value_updates_not_duplicates(s3, tmp_path):
    uri = str(tmp_path / "bronze")
    first = _extract(s3, {"UNRATE": payload(("2024-01-01", "3.7"))})
    fred_pipeline.load_raw_to_bronze(
        first, client=s3, table_uri=uri, storage_options={}
    )

    later = datetime(2024, 6, 10, 6, 0, tzinfo=timezone.utc)
    revised = _extract(s3, {"UNRATE": payload(("2024-01-01", "3.8"))}, run_ts=later)
    m = fred_pipeline.load_raw_to_bronze(
        revised, client=s3, table_uri=uri, storage_options={}
    )

    out = DeltaTable(uri).to_pandas()
    assert m["rows_updated"] == 1
    assert len(out) == 1
    assert out["value"].iloc[0] == 3.8


def test_load_stamps_run_id_and_reports_per_series_counts(s3, tmp_path):
    keys = _extract(
        s3,
        {
            "UNRATE": payload(("2024-01-01", "3.7"), ("2024-02-01", "3.9")),
            "HOUST": payload(("2024-03-01", "1400")),
        },
    )
    uri = str(tmp_path / "bronze")
    m = fred_pipeline.load_raw_to_bronze(
        keys, client=s3, table_uri=uri, storage_options={}, run_id="manual__x"
    )

    assert set(DeltaTable(uri).to_pandas()["run_id"]) == {"manual__x"}
    assert m["rows_per_series"] == {"HOUST": 1, "UNRATE": 2}
    assert (m["min_date"], m["max_date"]) == ("2024-01-01", "2024-03-01")
    json.dumps(m)  # must stay XCom-serializable


def test_load_rejects_invalid_data(s3, tmp_path):
    keys = _extract(s3, {"UNRATE": payload(("2024-01-01", "-5"))})
    with pytest.raises(DataQualityError, match=r"to_be_between\(value\)"):
        fred_pipeline.load_raw_to_bronze(
            keys, client=s3, table_uri=str(tmp_path / "b"), storage_options={}
        )
    assert not DeltaTable.is_deltatable(str(tmp_path / "b"))


# ── validation ─────────────────────────────────────────────────────────────


def _valid_df():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-02-01"]),
            "series_id": ["UNRATE", "UNRATE"],
            "series_name": ["Unemployment Rate"] * 2,
            "value": [3.7, 3.9],
            "ingested_at": [RUN_TS] * 2,
            "source_file": ["s3://raw/x"] * 2,
        }
    )


def test_validation_passes_on_good_data():
    assert validate_fred_observations(_valid_df(), ["UNRATE"])["success"]


def test_validation_fails_on_unknown_series():
    with pytest.raises(DataQualityError, match=r"expect_column_values_to_be_in_set"):
        validate_fred_observations(_valid_df(), ["HOUST"])


def test_validation_fails_on_duplicate_keys():
    df = _valid_df()
    df.loc[1, "date"] = df.loc[0, "date"]
    with pytest.raises(DataQualityError, match=r"expect_compound_columns_to_be_unique"):
        validate_fred_observations(df, ["UNRATE"])


def test_validation_fails_on_future_dates():
    df = _valid_df()
    df.loc[1, "date"] = pd.Timestamp.now() + pd.DateOffset(days=30)
    with pytest.raises(DataQualityError, match=r"to_be_between\(date\)"):
        validate_fred_observations(df, ["UNRATE"])
