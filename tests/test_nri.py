from unittest.mock import MagicMock, patch

import boto3
import pytest
import requests
from deltalake import DeltaTable
from moto import mock_aws

from ingestion import nri_client, nri_pipeline
from ingestion.validation import DataQualityError


def _row(i, **over):
    row = {
        "TRACTFIPS": f"{22019000100 + i}",
        "STCOFIPS": "22019",
        "BUILDVALUE": 1_000_000.0,
        "EAL_VALB": 2_800.0,
        "EAL_SCORE": 90.1,
    }
    row.update({f"{p}_EALB": 0.0 for p in nri_client.PERILS})
    row["HRCN_EALB"] = 1_400.0
    row.update(over)
    return row


def _resp(body, status=200):
    r = MagicMock(status_code=status)
    r.json.return_value = body
    r.raise_for_status.side_effect = (
        requests.HTTPError(str(status)) if status >= 400 else None
    )
    return r


def test_parse_keeps_property_loss_and_perils():
    df = nri_client.parse_tracts(
        [_row(0), _row(1, TRACTFIPS="1001020100")], "2025-12-18"
    )
    assert list(df["tract"]) == ["22019000100", "01001020100"]  # zero-padded
    assert df.loc[0, "eal_building_hurricane"] == 1400.0
    assert set(df["nri_release"]) == {"2025-12-18"}
    assert (
        "eal_score" in df and "RISK_SCORE" not in df
    )  # social-vulnerability composite left out


def test_parse_fails_loudly_on_missing_fields():
    row = _row(0)
    del row["HRCN_EALB"]
    with pytest.raises(ValueError, match="HRCN_EALB"):
        nri_client.parse_tracts([row], "x")


def test_arcgis_error_with_http_200_is_raised():
    body = {"error": {"code": 400, "message": "'outFields' parameter is invalid"}}
    with patch.object(nri_client.requests, "get", return_value=_resp(body)):
        with pytest.raises(nri_client.ArcGISError, match="outFields"):
            nri_client.fetch_all_pages(pause=0)


def test_fetch_pages_until_empty():
    pages = [
        _resp({"features": [{"attributes": _row(i)} for i in range(2000)]}),
        _resp({"features": [{"attributes": _row(i)} for i in range(5)]}),
        _resp({"features": []}),
    ]
    with patch.object(nri_client.requests, "get", side_effect=pages) as get:
        rows = nri_client.fetch_all_pages(pause=0)
    assert len(rows) == 2005
    assert [c.kwargs["params"]["resultOffset"] for c in get.call_args_list] == [
        0,
        2000,
        2005,
    ]


def test_release_date_from_item_metadata():
    with patch.object(
        nri_client.requests, "get", return_value=_resp({"modified": 1766016000000})
    ):
        assert nri_client.release_date() == "2025-12-18"


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
    with (
        patch.object(nri_pipeline, "release_date", return_value="2025-12-18"),
        patch.object(nri_pipeline, "fetch_all_pages", return_value=rows),
    ):
        return nri_pipeline.extract_to_raw(client=s3)


def test_land_and_load_idempotent(s3, tmp_path):
    uri = str(tmp_path / "nri")
    key = _land(s3, [_row(i) for i in range(80000)])
    assert key.startswith("fema/nri/2025-12-18/tracts_")
    m1 = nri_pipeline.load_raw_to_bronze(
        key, client=s3, table_uri=uri, storage_options={}
    )
    m2 = nri_pipeline.load_raw_to_bronze(
        key, client=s3, table_uri=uri, storage_options={}
    )
    assert m1["rows_inserted"] == 80000 and m1["tracts"] == 80000
    assert m2["rows_inserted"] == 0
    assert len(DeltaTable(uri).to_pandas()) == 80000


def test_partial_pull_rejected(s3, tmp_path):
    key = _land(s3, [_row(i) for i in range(1000)])
    with pytest.raises(DataQualityError, match="row_count"):
        nri_pipeline.load_raw_to_bronze(
            key, client=s3, table_uri=str(tmp_path / "n"), storage_options={}
        )
