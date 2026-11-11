import boto3
import pytest
from moto import mock_aws

from ingestion import storage


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("STORAGE_ACCESS_KEY", "test")
    monkeypatch.setenv("STORAGE_SECRET_KEY", "test")
    monkeypatch.setenv("STORAGE_REGION", "us-east-1")
    monkeypatch.delenv("STORAGE_ENDPOINT", raising=False)
    monkeypatch.setenv("RAW_BUCKET", "test-raw")
    monkeypatch.setenv("LAKE_BUCKET", "test-lake")
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


def test_ensure_buckets_creates_then_is_idempotent(s3):
    assert storage.ensure_buckets(client=s3) == ["test-raw", "test-lake"]
    assert storage.ensure_buckets(client=s3) == []
    names = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    assert names == {"test-raw", "test-lake"}


def test_put_raw_writes_and_returns_uri(s3):
    storage.ensure_buckets(client=s3)
    uri = storage.put_raw("fred/2024/01/01/x.json", b"{}", client=s3)

    assert uri == "s3://test-raw/fred/2024/01/01/x.json"
    body = s3.get_object(Bucket="test-raw", Key="fred/2024/01/01/x.json")["Body"]
    assert body.read() == b"{}"


def test_put_raw_refuses_overwrite(s3):
    storage.ensure_buckets(client=s3)
    storage.put_raw("k", b"1", client=s3)
    with pytest.raises(FileExistsError):
        storage.put_raw("k", b"2", client=s3)


def test_lake_uri(monkeypatch):
    monkeypatch.setenv("LAKE_BUCKET", "lake")
    assert storage.lake_uri("bronze", "fred") == "s3://lake/bronze/fred"


def test_delta_storage_options_http_endpoint(monkeypatch):
    monkeypatch.setenv("STORAGE_ACCESS_KEY", "a")
    monkeypatch.setenv("STORAGE_SECRET_KEY", "b")
    monkeypatch.setenv("STORAGE_ENDPOINT", "http://seaweedfs:8333")

    opts = storage.delta_storage_options()
    assert opts["AWS_ENDPOINT_URL"] == "http://seaweedfs:8333"
    assert opts["AWS_ALLOW_HTTP"] == "true"
    assert opts["AWS_ACCESS_KEY_ID"] == "a"


def test_missing_credentials_raise(monkeypatch):
    monkeypatch.delenv("STORAGE_ACCESS_KEY", raising=False)
    with pytest.raises(ValueError, match="STORAGE_ACCESS_KEY"):
        storage.get_s3_client()
