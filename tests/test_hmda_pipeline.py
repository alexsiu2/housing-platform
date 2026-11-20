import gzip
import io
import zipfile
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

from ingestion import hmda_pipeline

CSV = b"activity_year,census_tract\n2023,36061000100\n2023,36061000200\n"


def _zip_bytes(name="2023_public_lar_csv.csv", content=CSV, extra=None):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, content)
        if extra:
            zf.writestr(extra, b"x")
    return buf.getvalue()


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("STORAGE_ACCESS_KEY", "test")
    monkeypatch.setenv("STORAGE_SECRET_KEY", "test")
    monkeypatch.delenv("STORAGE_ENDPOINT", raising=False)
    monkeypatch.setenv("RAW_BUCKET", "raw")
    monkeypatch.setenv("LAKE_BUCKET", "lake")
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="raw")
        client.create_bucket(Bucket="lake")
        yield client


def _response(body: bytes, length=None):
    r = MagicMock()
    r.__enter__.return_value = r
    r.headers = {"Content-Length": str(len(body) if length is None else length)}
    r.raw = io.BytesIO(body)
    return r


def test_download_streams_zip_untouched_then_reuses(s3):
    body = _zip_bytes()
    with patch.object(
        hmda_pipeline.requests, "get", return_value=_response(body)
    ) as get:
        first = hmda_pipeline.download_to_raw(2023, client=s3)
        second = hmda_pipeline.download_to_raw(2023, client=s3)
    assert first == {"key": "hmda/2023/2023_public_lar_csv.zip", "bytes": len(body),
                     "downloaded": True}  # fmt: skip
    assert second["downloaded"] is False and get.call_count == 1
    assert s3.get_object(Bucket="raw", Key=first["key"])["Body"].read() == body


def test_truncated_download_is_removed(s3):
    body = _zip_bytes()
    resp = _response(body, length=len(body) + 100)
    with patch.object(hmda_pipeline.requests, "get", return_value=resp):
        with pytest.raises(IOError, match="expected"):
            hmda_pipeline.download_to_raw(2023, client=s3)
    assert hmda_pipeline._exists(s3, "raw", hmda_pipeline.raw_key(2023)) is None


def test_rejects_pre_2018_years(s3):
    with pytest.raises(ValueError, match="2018"):
        hmda_pipeline.download_to_raw(2017, client=s3)


def test_stage_recompresses_csv_to_gzip_and_clears(s3, tmp_path):
    s3.put_object(Bucket="raw", Key=hmda_pipeline.raw_key(2023), Body=_zip_bytes())
    info = hmda_pipeline.stage_for_spark(2023, client=s3, work_dir=str(tmp_path))
    assert info["staged"] == "s3a://lake/staging/hmda/2023/"
    gz = s3.get_object(Bucket="lake", Key=hmda_pipeline.staging_key(2023))[
        "Body"
    ].read()
    assert gzip.decompress(gz) == CSV
    assert list(tmp_path.iterdir()) == []  # temp files cleaned up
    hmda_pipeline.clear_staging(2023, client=s3)
    assert hmda_pipeline._exists(s3, "lake", hmda_pipeline.staging_key(2023)) is None


def test_stage_rejects_unexpected_zip_contents(s3, tmp_path):
    s3.put_object(
        Bucket="raw", Key=hmda_pipeline.raw_key(2023), Body=_zip_bytes(extra="b.csv")
    )
    with pytest.raises(ValueError, match="expected 1 CSV"):
        hmda_pipeline.stage_for_spark(2023, client=s3, work_dir=str(tmp_path))
