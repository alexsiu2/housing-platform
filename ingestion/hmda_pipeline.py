"""
HMDA snapshot files: download -> raw zone -> staged .csv.gz for Spark.

  download_to_raw(year)  FFIEC snapshot zip (0.6-1.5 GB) streamed into
                         s3://rpof-raw/hmda/<year>/ untouched. A year's
                         snapshot is fixed, so an existing file is reused.
  stage_for_spark(year)  Spark can't read .zip; re-compress the single CSV
                         member to s3://rpof-lake/staging/hmda/<year>/*.csv.gz.
  clear_staging(year)    Remove the staged copy once Bronze is written.

Everything streams through temp files on disk; nothing loads a file into
memory. Spark then runs jobs/hmda_lar.py on the staged copy.
"""

import gzip
import logging
import os
import shutil
import tempfile
import zipfile

import requests
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from ingestion.storage import get_s3_client, lake_bucket, raw_bucket

logger = logging.getLogger(__name__)

SNAPSHOT_URL = (
    "https://files.ffiec.cfpb.gov/static-data/snapshot/{year}/{year}_public_lar_csv.zip"
)
FIRST_YEAR = 2018  # current LAR format (2018 HMDA rule changes)
REQUEST_TIMEOUT_SECONDS = (30, 600)  # connect, read
USER_AGENT = "rpof-research/0.1 (personal non-commercial research)"
TRANSFER = TransferConfig(multipart_chunksize=64 * 1024 * 1024, max_concurrency=4)


def raw_key(year: int) -> str:
    return f"hmda/{year}/{year}_public_lar_csv.zip"


def staging_key(year: int) -> str:
    return f"staging/hmda/{year}/{year}_public_lar.csv.gz"


def staging_uri(year: int) -> str:
    """Directory Spark reads (s3a), e.g. s3a://rpof-lake/staging/hmda/2023/."""
    return f"s3a://{lake_bucket()}/staging/hmda/{year}/"


def _exists(client, bucket: str, key: str) -> int | None:
    """Object size if it exists, else None."""
    try:
        return client.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def download_to_raw(year: int, client=None) -> dict:
    """Stream the FFIEC snapshot zip into the raw zone (skip if already there)."""
    if year < FIRST_YEAR:
        raise ValueError(f"HMDA LAR format used here starts in {FIRST_YEAR}")
    client = client or get_s3_client()
    bucket, key = raw_bucket(), raw_key(year)
    existing = _exists(client, bucket, key)
    if existing:
        logger.info("Raw HMDA %d already landed (%d bytes); reusing", year, existing)
        return {"key": key, "bytes": existing, "downloaded": False}

    url = SNAPSHOT_URL.format(year=year)
    with requests.get(
        url,
        stream=True,
        timeout=REQUEST_TIMEOUT_SECONDS,
        headers={"User-Agent": USER_AGENT},
    ) as r:
        r.raise_for_status()
        expected = int(r.headers.get("Content-Length", 0))
        r.raw.decode_content = False  # keep the zip bytes exactly as served
        client.upload_fileobj(r.raw, bucket, key, Config=TRANSFER)

    size = _exists(client, bucket, key)
    if expected and size != expected:
        client.delete_object(Bucket=bucket, Key=key)
        raise IOError(f"HMDA {year}: landed {size} bytes, expected {expected}")
    logger.info("Landed s3://%s/%s (%d bytes)", bucket, key, size)
    return {"key": key, "bytes": size, "downloaded": True}


def stage_for_spark(year: int, client=None, work_dir: str | None = None) -> dict:
    """Re-compress the zip's single CSV to gzip in the lake staging area."""
    client = client or get_s3_client()
    work = tempfile.mkdtemp(prefix=f"hmda_{year}_", dir=work_dir)
    try:
        local_zip = os.path.join(work, "lar.zip")
        client.download_file(raw_bucket(), raw_key(year), local_zip, Config=TRANSFER)
        with zipfile.ZipFile(local_zip) as zf:
            members = [m for m in zf.infolist() if m.filename.endswith(".csv")]
            if len(members) != 1:
                raise ValueError(f"HMDA {year}: expected 1 CSV in zip, got {members}")
            member = members[0]
            local_gz = os.path.join(work, "lar.csv.gz")
            with zf.open(member) as src, gzip.open(
                local_gz, "wb", compresslevel=3
            ) as dst:
                shutil.copyfileobj(src, dst, length=16 * 1024 * 1024)
        os.remove(local_zip)
        client.upload_file(local_gz, lake_bucket(), staging_key(year), Config=TRANSFER)
        info = {
            "year": year,
            "csv_bytes": member.file_size,
            "gz_bytes": os.path.getsize(local_gz),
            "staged": staging_uri(year),
        }
        logger.info("Staged HMDA %s", info)
        return info
    finally:
        shutil.rmtree(work, ignore_errors=True)


def clear_staging(year: int, client=None) -> None:
    client = client or get_s3_client()
    client.delete_object(Bucket=lake_bucket(), Key=staging_key(year))
