"""
RSS feeds -> Kafka topic news.rss.v1 (design doc §9.1).

Every poll_minutes, fetch each feed in config/rss_feeds.yaml (conditional GET,
robots.txt honoured) and publish items not sent since the producer started.
Only the headline, link, date and feed summary are kept.

    python -m streaming.rss_producer           # run forever
    python -m streaming.rss_producer --once    # one poll, then exit
"""

import argparse
import calendar
import hashlib
import html
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import yaml

from streaming.common import (
    SNIPPET_MAX_CHARS,
    NewsProducer,
    PoliteFetcher,
    SeenCache,
    run_forever,
    setup_logging,
    utc_now,
)

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "rss_feeds.yaml"
_TAGS = re.compile(r"<[^>]+>")
_SPACE = re.compile(r"\s+")


def load_config(path: Path = CONFIG_PATH) -> dict:
    return yaml.safe_load(path.read_text())


def clean_text(value: str | None, limit: int | None = None) -> str | None:
    """Strip HTML tags/entities and collapse whitespace."""
    if not value:
        return None
    text = _SPACE.sub(" ", html.unescape(_TAGS.sub(" ", value))).strip()
    if limit and len(text) > limit:
        text = text[: limit - 1].rstrip() + "…"
    return text or None


def _entry_time(entry) -> datetime | None:
    for field in ("published_parsed", "updated_parsed"):
        parsed = entry.get(field)
        if parsed:  # feedparser normalizes to UTC struct_time
            return datetime.fromtimestamp(calendar.timegm(parsed), tz=timezone.utc)
    return None


def source_id_for(feed_name: str, entry) -> str:
    """Stable id: hash of the feed's guid (or link), namespaced by feed."""
    ident = entry.get("id") or entry.get("link") or entry.get("title") or ""
    return hashlib.sha1(f"{feed_name}|{ident}".encode()).hexdigest()


def parse_feed(content: bytes, feed_name: str, fetched_at: datetime) -> list[dict]:
    """Feed bytes -> NewsItem records (dicts matching news_item.avsc)."""
    parsed = feedparser.parse(content)
    records = []
    for entry in parsed.entries:
        url = entry.get("link")
        title = clean_text(entry.get("title"))
        if not url or not title:
            continue
        records.append(
            {
                "source": "rss",
                "source_id": source_id_for(feed_name, entry),
                "url": url,
                "title": title,
                "body_snippet": clean_text(entry.get("summary"), SNIPPET_MAX_CHARS),
                "published_at": _entry_time(entry),
                "fetched_at": fetched_at,
                "subreddit": None,
                "feed_name": feed_name,
                "geo_hint": None,
                "engagement": None,
                "tone": None,
                "themes": [],
            }
        )
    return records


class RssPoller:
    def __init__(self, config: dict, producer, fetcher: PoliteFetcher | None = None):
        self.feeds = config["feeds"]
        self.producer = producer
        self.fetcher = fetcher or PoliteFetcher()
        self.seen = SeenCache()

    def poll_once(self) -> int:
        sent = 0
        for feed in self.feeds:
            try:
                content = self.fetcher.get(feed["url"])
            except Exception as exc:  # one bad feed never stops the others
                logger.warning("Feed %s failed: %s", feed["name"], exc)
                continue
            if content is None:
                continue
            for rec in parse_feed(content, feed["name"], utc_now()):
                if self.seen.add_if_new(rec["source_id"]):
                    self.producer.send(rec)
                    sent += 1
        self.producer.flush()
        logger.info("RSS poll: sent %d new items from %d feeds", sent, len(self.feeds))
        return sent


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    p.add_argument("--once", action="store_true", help="poll once and exit")
    args = p.parse_args(argv)
    setup_logging()
    config = load_config()
    poller = RssPoller(config, NewsProducer("rss"))
    if args.once:
        poller.poll_once()
    else:
        run_forever(poller.poll_once, int(config.get("poll_minutes", 15)) * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
