"""Spark logic of jobs/stream_news_to_bronze.py on a local Spark session.

Skipped where PySpark/Java aren't installed (e.g. the dev venv); runs in CI's
Spark job and inside the Airflow image. Avro decoding needs spark-avro: the
Airflow image has it in /opt/rpof-spark-jars; elsewhere it is fetched from
Maven Central (spark.jars.packages). Packages load only when the Spark JVM
starts, so run this file in its own pytest process (as CI does).
"""

import io
import json
import struct
from datetime import datetime, timezone
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
fastavro = pytest.importorskip("fastavro")

from pyspark.sql import SparkSession  # noqa: E402

from jobs import stream_news_to_bronze as job  # noqa: E402

SCHEMA_V1 = json.loads(
    (Path(__file__).parents[1] / "streaming/schemas/news_item.avsc").read_text()
)
# An older writer schema without the optional tone/themes fields.
SCHEMA_V0 = {
    **SCHEMA_V1,
    "fields": [f for f in SCHEMA_V1["fields"] if f["name"] not in ("tone", "themes")],
}
LOCAL_AVRO_JAR = Path("/opt/rpof-spark-jars/spark-avro_2.13-4.2.0.jar")


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("test_stream_news")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if LOCAL_AVRO_JAR.exists():
        builder = builder.config("spark.jars", str(LOCAL_AVRO_JAR))
    else:
        builder = builder.config(
            "spark.jars.packages", "org.apache.spark:spark-avro_2.13:4.2.0"
        )
    s = builder.getOrCreate()
    yield s
    s.stop()


def wire(schema: dict, schema_id: int, record: dict) -> bytes:
    """Confluent wire format: magic 0, 4-byte schema id, Avro body."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, fastavro.parse_schema(schema), record)
    return b"\x00" + struct.pack(">I", schema_id) + buf.getvalue()


def record(source_id: str, **extra) -> dict:
    t = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
    rec = {
        "source": "rss", "source_id": source_id, "url": f"https://x/{source_id}",
        "title": f"title {source_id}", "body_snippet": None, "published_at": t,
        "fetched_at": t, "subreddit": None, "feed_name": "f", "geo_hint": None,
        "engagement": None,
    }  # fmt: skip
    rec.update(extra)
    return rec


def kafka_df(spark, values: list[bytes]):
    ts = datetime(2026, 11, 20, 12, 5)
    rows = [("news.rss.v1", 0, i, ts, bytearray(v)) for i, v in enumerate(values)]
    return spark.createDataFrame(
        rows,
        "topic string, partition int, offset long, timestamp timestamp, value binary",
    )


def test_split_wire_format(spark):
    df = kafka_df(spark, [wire(SCHEMA_V1, 7, record("a", tone=1.0, themes=[])), b"{}"])
    out = job.split_wire_format(df).orderBy("offset").collect()
    assert [r.has_magic for r in out] == [True, False]
    assert out[0].schema_id == 7


def test_decode_mixes_schema_versions_and_dedupes(spark):
    values = [
        wire(SCHEMA_V0, 1, record("a", title="old")),
        wire(SCHEMA_V1, 2, record("b", tone=-2.5, themes=["ECON_HOUSING_PRICES"])),
        wire(SCHEMA_V1, 2, record("a", title="new", tone=0.0, themes=[])),
    ]
    good = job.split_wire_format(kafka_df(spark, values))
    decoded = job.decode(good, {1: json.dumps(SCHEMA_V0), 2: json.dumps(SCHEMA_V1)})
    assert decoded.count() == 3
    rows = {
        r.source_id: r
        for r in job.to_bronze_rows(job.latest_per_key(decoded), "t").collect()
    }
    assert set(rows) == {"a", "b"}
    assert rows["a"].title == "new"  # highest offset wins
    assert rows["b"].tone == -2.5 and rows["b"].themes == ["ECON_HOUSING_PRICES"]
    assert rows["a"].published_at == datetime(2026, 11, 20, 12, 0)  # naive UTC


def test_table_ddl_matches_bronze_columns():
    ddl = job.table_ddl("s3a://lake/bronze/news_events_raw")
    for col in list(job.NEWS_COLUMNS) + list(job.EXTRA_COLUMNS):
        assert f"`{col}`" in ddl
    assert "PARTITIONED BY (source)" in ddl
