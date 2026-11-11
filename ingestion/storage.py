"""
Object storage helpers for the raw landing zone and the Delta lake.

Locally this talks to SeaweedFS (S3-compatible, started by docker/compose.yaml).
In the cloud phase the same code points at GCS's S3-compatible endpoint, so
only environment variables change.

Environment variables (see .env.example):
    STORAGE_ENDPOINT      e.g. http://localhost:8333
    STORAGE_ACCESS_KEY
    STORAGE_SECRET_KEY
    STORAGE_REGION        default us-east-1
    RAW_BUCKET            default rpof-raw
    LAKE_BUCKET           default rpof-lake
"""

import argparse
import logging
import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_REGION = "us-east-1"


def raw_bucket() -> str:
    return os.getenv("RAW_BUCKET", "rpof-raw")


def lake_bucket() -> str:
    return os.getenv("LAKE_BUCKET", "rpof-lake")


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} is not set. Add it to your .env file.")
    return value


def get_s3_client():
    """Return a boto3 S3 client for the configured endpoint."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("STORAGE_ENDPOINT") or None,
        aws_access_key_id=_required("STORAGE_ACCESS_KEY"),
        aws_secret_access_key=_required("STORAGE_SECRET_KEY"),
        region_name=os.getenv("STORAGE_REGION", DEFAULT_REGION),
    )


def ensure_buckets(client=None, buckets: list[str] | None = None) -> list[str]:
    """
    Create the raw and lake buckets if they don't exist. Safe to re-run.

    Returns the list of buckets that were newly created.
    """
    client = client or get_s3_client()
    buckets = buckets or [raw_bucket(), lake_bucket()]
    created = []
    for bucket in buckets:
        try:
            client.head_bucket(Bucket=bucket)
            logger.info("Bucket %s already exists", bucket)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code not in ("404", "NoSuchBucket", "NotFound"):
                raise
            client.create_bucket(Bucket=bucket)
            created.append(bucket)
            logger.info("Created bucket %s", bucket)
    return created


def put_raw(key: str, body: bytes, client=None, bucket: str | None = None) -> str:
    """
    Write a file to the raw landing zone and return its s3:// URI.

    The raw zone is immutable: callers must build keys that are unique per
    run (e.g. include the run date), and this refuses to overwrite.
    """
    client = client or get_s3_client()
    bucket = bucket or raw_bucket()
    try:
        client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey"):
            raise
    else:
        raise FileExistsError(f"s3://{bucket}/{key} already exists; raw is immutable")

    client.put_object(Bucket=bucket, Key=key, Body=body)
    uri = f"s3://{bucket}/{key}"
    logger.info("Landed %d bytes at %s", len(body), uri)
    return uri


def lake_uri(layer: str, table: str) -> str:
    """URI of a Delta table in the lake, e.g. lake_uri('bronze', 'fred_series_raw')."""
    return f"s3://{lake_bucket()}/{layer}/{table}"


def delta_storage_options() -> dict[str, str]:
    """
    Storage options for deltalake (delta-rs) pointing at the same endpoint.

    AWS_S3_ALLOW_UNSAFE_RENAME is acceptable here because each Bronze table has
    a single writer (one Airflow task at a time). Revisit before allowing
    concurrent writers.
    """
    options = {
        "AWS_ACCESS_KEY_ID": _required("STORAGE_ACCESS_KEY"),
        "AWS_SECRET_ACCESS_KEY": _required("STORAGE_SECRET_KEY"),
        "AWS_REGION": os.getenv("STORAGE_REGION", DEFAULT_REGION),
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    endpoint = os.getenv("STORAGE_ENDPOINT")
    if endpoint:
        options["AWS_ENDPOINT_URL"] = endpoint
        if endpoint.startswith("http://"):
            options["AWS_ALLOW_HTTP"] = "true"
    return options


def main() -> None:
    parser = argparse.ArgumentParser(description="Storage utilities")
    parser.add_argument(
        "--create-buckets", action="store_true", help="create raw + lake buckets"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.create_buckets:
        ensure_buckets()


if __name__ == "__main__":
    main()
