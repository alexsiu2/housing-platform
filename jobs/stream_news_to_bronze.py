"""
Kafka news.*.v1 topics -> bronze.news_events_raw (design doc §9.1).

Spark Structured Streaming with trigger(availableNow): each run reads every
event since the last checkpoint, writes it, and exits. Airflow starts it
every 15 minutes (dags/stream_news_to_bronze.py), so no Spark process sits
idle between runs (D16).

Per micro-batch (foreachBatch):
  1. Split the Confluent wire format: magic byte 0, a 4-byte Schema Registry
     id, then the Avro body. Records without the magic byte are counted and
     dropped.
  2. Decode each schema id's records with from_avro, using the writer schema
     fetched from Schema Registry, so older schema versions still decode.
  3. Keep one row per (source, source_id), the latest Kafka offset.
  4. MERGE into Delta, insert-only on (source, source_id): replaying a batch
     after a crash changes nothing, so with the checkpoint the sink is
     effectively exactly-once.

    spark-submit jobs/stream_news_to_bronze.py --run-id manual
"""

import argparse
import json
import os
import urllib.request
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TOPIC_PATTERN = r"news\..+\.v1"
KEY_COLUMNS = ["source", "source_id"]

# Bronze columns from the Avro record (news_item.avsc), in order.
NEWS_COLUMNS = {
    "source": "string",
    "source_id": "string",
    "url": "string",
    "title": "string",
    "body_snippet": "string",
    "published_at": "timestamp",
    "fetched_at": "timestamp",
    "subreddit": "string",
    "feed_name": "string",
    "geo_hint": "string",
    "engagement": "bigint",
    "tone": "double",
    "themes": "array<string>",
}


def split_wire_format(raw: DataFrame) -> DataFrame:
    """Kafka rows -> schema_id + Avro payload (+ Kafka position), valid only."""
    return raw.select(
        (F.substring("value", 1, 1) == F.lit(b"\x00")).alias("has_magic"),
        F.conv(F.hex(F.substring("value", 2, 4)), 16, 10)
        .cast("int")
        .alias("schema_id"),
        F.expr("substring(value, 6, length(value) - 5)").alias("payload"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
    )


def fetch_schema(registry_url: str, schema_id: int) -> str:
    """The writer schema (Avro JSON) for a Schema Registry id."""
    with urllib.request.urlopen(
        f"{registry_url}/schemas/ids/{schema_id}", timeout=30
    ) as resp:
        return json.loads(resp.read())["schema"]


def decode(wire: DataFrame, schemas: dict[int, str]) -> DataFrame:
    """Decode each schema id with its writer schema; fill fields it lacks."""
    from pyspark.sql.avro.functions import from_avro

    parts = []
    for schema_id, schema in schemas.items():
        rec = wire.where(F.col("schema_id") == schema_id).withColumn(
            "rec", from_avro("payload", schema, {"mode": "FAILFAST"})
        )
        fields = set(rec.select("rec.*").columns)
        cols = [
            (
                F.col(f"rec.{name}").cast(dtype)
                if name in fields
                else F.lit(None).cast(dtype)
            ).alias(name)
            for name, dtype in NEWS_COLUMNS.items()
        ]
        parts.append(
            rec.select(*cols, "topic", "partition", "offset", "kafka_timestamp")
        )
    out = parts[0]
    for part in parts[1:]:
        out = out.unionByName(part)
    return out


def latest_per_key(df: DataFrame) -> DataFrame:
    """One row per (source, source_id): the one with the highest offset."""
    w = Window.partitionBy(*KEY_COLUMNS).orderBy(
        F.col("partition").desc(), F.col("offset").desc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def to_bronze_rows(df: DataFrame, run_id: str) -> DataFrame:
    """Lake convention: timestamps stored tz-naive in UTC (session tz is UTC)."""
    return (
        df.withColumn("published_at", F.col("published_at").cast("timestamp_ntz"))
        .withColumn("fetched_at", F.col("fetched_at").cast("timestamp_ntz"))
        .withColumn("kafka_timestamp", F.col("kafka_timestamp").cast("timestamp_ntz"))
        .withColumn("ingested_at", F.current_timestamp().cast("timestamp_ntz"))
        .withColumn("run_id", F.lit(run_id))
    )


# Bronze-only columns added after decoding (see to_bronze_rows).
EXTRA_COLUMNS = {
    "topic": "string",
    "partition": "int",
    "offset": "bigint",
    "kafka_timestamp": "timestamp_ntz",
    "ingested_at": "timestamp_ntz",
    "run_id": "string",
}


def table_ddl(table_path: str) -> str:
    """CREATE TABLE IF NOT EXISTS for the Bronze table (Spark SQL, no Delta API)."""
    types = {**NEWS_COLUMNS, **EXTRA_COLUMNS}
    types["published_at"] = types["fetched_at"] = "timestamp_ntz"
    cols = ", ".join(f"`{name}` {dtype}" for name, dtype in types.items())
    return (
        f"CREATE TABLE IF NOT EXISTS delta.`{table_path}` ({cols}) "
        "USING delta PARTITIONED BY (source)"
    )


def merge_into_bronze(spark: SparkSession, rows: DataFrame, table_path: str) -> int:
    """Insert rows whose (source, source_id) is new; returns rows inserted."""
    spark.sql(table_ddl(table_path))

    def last_commit():
        return (
            spark.sql(f"DESCRIBE HISTORY delta.`{table_path}` LIMIT 1")
            .select("version", "operationMetrics")
            .first()
        )

    before = last_commit().version
    rows.createOrReplaceTempView("news_batch")
    on = " and ".join(f"t.{k} = s.{k}" for k in KEY_COLUMNS)
    spark.sql(
        f"MERGE INTO delta.`{table_path}` t USING news_batch s ON {on} "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    after = last_commit()
    # A MERGE that inserts nothing writes no commit; the latest history entry
    # is then an older one, whose metrics must not be counted again.
    if after.version == before:
        return 0
    return int(after.operationMetrics.get("numTargetRowsInserted", 0))


class BatchWriter:
    """foreachBatch sink; keeps totals for the run summary."""

    def __init__(self, table_path: str, registry_url: str, run_id: str):
        self.table_path = table_path
        self.registry_url = registry_url
        self.run_id = run_id
        self.totals = {"batches": 0, "events": 0, "bad_wire_format": 0, "inserted": 0}

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        spark = batch.sparkSession
        wire = split_wire_format(batch).cache()
        bad = wire.where(~F.col("has_magic")).count()
        good = wire.where(F.col("has_magic"))
        ids = [r.schema_id for r in good.select("schema_id").distinct().collect()]
        self.totals["batches"] += 1
        self.totals["bad_wire_format"] += bad
        if ids:
            schemas = {i: fetch_schema(self.registry_url, i) for i in ids}
            decoded = decode(good, schemas)
            self.totals["events"] += decoded.count()
            rows = to_bronze_rows(latest_per_key(decoded), self.run_id)
            self.totals["inserted"] += merge_into_bronze(spark, rows, self.table_path)
        wire.unpersist()


def build_spark(app_name: str) -> SparkSession:
    endpoint = os.environ.get("STORAGE_ENDPOINT", "http://seaweedfs:8333")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Readable by delta-rs/DuckDB: no deletion vectors.
        .config(
            "spark.databricks.delta.properties.defaults.enableDeletionVectors", "false"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")  # tiny batches
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(endpoint.startswith("https")).lower(),
        )
        .config("spark.hadoop.fs.s3a.access.key", os.environ["STORAGE_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["STORAGE_SECRET_KEY"])
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            os.environ.get("STORAGE_REGION", "us-east-1"),
        )
        .getOrCreate()
    )


def run(spark: SparkSession, args) -> dict:
    writer = BatchWriter(
        f"{args.lake}/bronze/news_events_raw", args.schema_registry, args.run_id
    )
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        # Topics keep a week; if this job was off longer than that, carry on
        # from what is left instead of failing (GDELT is backfilled weekly).
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .load()
    )
    query = (
        stream.writeStream.foreachBatch(writer)
        .option("checkpointLocation", f"{args.lake}/_checkpoints/stream_news_to_bronze")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return writer.totals


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Kafka news topics -> Bronze Delta")
    p.add_argument(
        "--lake", default=f"s3a://{os.environ.get('LAKE_BUCKET', 'rpof-lake')}"
    )
    p.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    p.add_argument(
        "--schema-registry",
        default=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
    )
    p.add_argument("--max-offsets", type=int, default=50_000)
    p.add_argument(
        "--run-id", default=f"manual_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    )
    args = p.parse_args(argv)
    spark = build_spark("stream_news_to_bronze")
    try:
        totals = run(spark, args)
    finally:
        spark.stop()
    print("STREAM_SUMMARY " + json.dumps(totals))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
