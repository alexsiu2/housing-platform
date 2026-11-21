"""
Shared pieces for the news producers (design doc §9.1).

- Polite HTTP: a descriptive user agent, robots.txt checked per host, and
  conditional GETs (ETag / Last-Modified) so an unchanged feed costs one 304.
- Avro on Kafka: values are serialized against streaming/schemas/news_item.avsc
  through Schema Registry (Confluent wire format), and each subject is held to
  BACKWARD compatibility so the schema can only evolve safely.
- A poll loop that stops cleanly on SIGTERM (docker stop).

Kafka is a replayable buffer, not storage: Bronze Delta is the source of truth,
and the stream job dedupes on (source, source_id), so re-sending an item after
a restart is harmless.
"""

import logging
import os
import signal
import threading
import time
import urllib.robotparser
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import requests

logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).with_name("schemas") / "news_item.avsc"
USER_AGENT = "rpof-news-reader/0.1 (personal research project; non-commercial)"
SNIPPET_MAX_CHARS = 1000
TOPIC_RETENTION_MS = 7 * 24 * 3600 * 1000  # a week of replay; Delta keeps history


def topic_for(source: str) -> str:
    """Topic names carry a version suffix (design §9.2)."""
    return f"news.{source}.v1"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PoliteFetcher:
    """HTTP GET that honours robots.txt and skips unchanged resources."""

    def __init__(self, session: requests.Session | None = None, timeout: int = 30):
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.timeout = timeout
        self._robots: dict[str, urllib.robotparser.RobotFileParser | None] = {}
        self._validators: dict[str, dict[str, str]] = {}

    def allowed(self, url: str) -> bool:
        parts = urlsplit(url)
        host = f"{parts.scheme}://{parts.netloc}"
        if host not in self._robots:
            parser = urllib.robotparser.RobotFileParser()
            try:
                resp = self.session.get(f"{host}/robots.txt", timeout=self.timeout)
            except requests.RequestException:
                resp = None
            if resp is not None and resp.status_code == 200:
                parser.parse(resp.text.splitlines())
                self._robots[host] = parser
            elif resp is not None and resp.status_code in (401, 403):
                parser.disallow_all = True  # robots.txt itself is forbidden
                self._robots[host] = parser
            else:
                self._robots[host] = None  # no robots.txt: allowed
        parser = self._robots[host]
        return parser is None or parser.can_fetch(USER_AGENT, url)

    def get(self, url: str) -> bytes | None:
        """Body, or None when robots.txt disallows it or nothing changed (304)."""
        if not self.allowed(url):
            logger.warning("robots.txt disallows %s; skipping", url)
            return None
        resp = self.session.get(
            url, headers=self._validators.get(url, {}), timeout=self.timeout
        )
        if resp.status_code == 304:
            return None
        resp.raise_for_status()
        validators = {}
        if resp.headers.get("ETag"):
            validators["If-None-Match"] = resp.headers["ETag"]
        if resp.headers.get("Last-Modified"):
            validators["If-Modified-Since"] = resp.headers["Last-Modified"]
        self._validators[url] = validators
        return resp.content


class SeenCache:
    """Bounded memory of recently sent keys, so each poll sends only new items."""

    def __init__(self, max_size: int = 50_000):
        self.max_size = max_size
        self._keys: OrderedDict[str, None] = OrderedDict()

    def add_if_new(self, key: str) -> bool:
        if key in self._keys:
            self._keys.move_to_end(key)
            return False
        self._keys[key] = None
        if len(self._keys) > self.max_size:
            self._keys.popitem(last=False)
        return True


def kafka_config() -> dict:
    return {
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    }


def schema_registry_url() -> str:
    return os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")


def ensure_topic(topic: str) -> None:
    """Create the topic if missing (auto-create is off on the broker)."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient(kafka_config())
    if topic in admin.list_topics(timeout=15).topics:
        return
    futures = admin.create_topics(
        [
            NewTopic(
                topic,
                num_partitions=1,
                replication_factor=1,
                config={"retention.ms": str(TOPIC_RETENTION_MS)},
            )
        ]
    )
    for name, fut in futures.items():
        try:
            fut.result()
            logger.info("Created topic %s", name)
        except Exception as exc:  # created concurrently by the other producer
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


class NewsProducer:
    """Kafka producer for one source's topic, Avro values, string keys."""

    def __init__(self, source: str):
        from confluent_kafka import SerializingProducer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import StringSerializer

        self.source = source
        self.topic = topic_for(source)
        ensure_topic(self.topic)
        registry = SchemaRegistryClient({"url": schema_registry_url()})
        subject = f"{self.topic}-value"
        registry.set_compatibility(subject_name=subject, level="BACKWARD")
        self._producer = SerializingProducer(
            {
                **kafka_config(),
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": AvroSerializer(
                    registry, SCHEMA_PATH.read_text(), lambda rec, ctx: rec
                ),
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 200,
            }
        )
        self.delivered = 0
        self.failed = 0

    def _on_delivery(self, err, msg) -> None:
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            logger.error("Delivery failed for %s: %s", msg.key(), err)

    def send(self, record: dict) -> None:
        key = f"{record['source']}:{record['source_id']}"
        self._producer.produce(
            self.topic, key=key, value=record, on_delivery=self._on_delivery
        )
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(30)


def run_forever(poll_once, interval_seconds: int) -> None:
    """Call poll_once() every interval until SIGTERM/SIGINT; errors are logged."""
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    while not stop.is_set():
        started = time.monotonic()
        try:
            poll_once()
        except Exception:  # keep the service up; the next poll retries
            logger.exception("Poll failed")
        stop.wait(max(5.0, interval_seconds - (time.monotonic() - started)))
    logger.info("Stopping")


def setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    # The Schema Registry client logs every HTTP request at INFO.
    for noisy in ("httpx", "httpx2", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
