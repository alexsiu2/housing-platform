"""
Live GDELT GKG (every 15 minutes) -> Kafka topic news.gdelt.v1 (design §9.1).

GDELT publishes a new GKG file every 15 minutes (~2-10 MB zipped) and lists
it in lastupdate.txt with its MD5. Each poll downloads the newest file if it
is new, checks the MD5, and keeps the same rows as the weekly BigQuery pull
(ingestion/gdelt_client.py): web articles with a housing theme from
ingestion/gdelt_themes.py and a US location. Each one is published with its
page title, tone and US city locations (for metro tagging in dbt).

If the producer is down for a while it does not catch up on missed files:
the weekly BigQuery DAG (ingest_gdelt_news) is the complete record; this
stream adds same-week freshness and page titles for the LLM outlook.

    python -m streaming.gdelt_producer           # run forever
    python -m streaming.gdelt_producer --once    # one poll, then exit
"""

import argparse
import csv
import hashlib
import html
import io
import logging
import re
import zipfile
from datetime import datetime, timezone

from ingestion.gdelt_themes import HOUSING_THEMES
from streaming.common import (
    NewsProducer,
    PoliteFetcher,
    SeenCache,
    run_forever,
    setup_logging,
    utc_now,
)

logger = logging.getLogger(__name__)

LASTUPDATE_URL = "https://data.gdeltproject.org/gdeltv2/lastupdate.txt"
POLL_SECONDS = 15 * 60

# GKG 2.1 column positions (27 tab-separated columns).
COL_ID, COL_DATE, COL_COLLECTION, COL_SOURCE, COL_URL = 0, 1, 2, 3, 4
COL_THEMES, COL_LOCATIONS, COL_TONE, COL_EXTRAS = 7, 9, 15, 26
GKG_COLUMNS = 27
WEB_COLLECTION = "1"  # SourceCollectionIdentifier: 1 = web
US_CITY = "3"  # v1 location types: 1 country, 2 US state, 3 US city, 4 world city

_HOUSING = frozenset(HOUSING_THEMES)
_TITLE = re.compile(r"<PAGE_TITLE>(.*?)</PAGE_TITLE>", re.S)

csv.field_size_limit(2**31 - 1)


class ChecksumMismatch(RuntimeError):
    pass


def parse_lastupdate(text: str) -> dict | None:
    """The GKG line of lastupdate.txt -> {size, md5, url} (url forced to https)."""
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[2].endswith(".gkg.csv.zip"):
            size, md5, url = parts
            return {
                "size": int(size),
                "md5": md5,
                "url": url.replace("http://", "https://", 1),
            }
    return None


def us_cities(locations: str) -> list[str]:
    """v1 Locations -> 'Full name|ADM1|lat|long' for each US city, deduped."""
    out = []
    for loc in locations.split(";"):
        f = loc.split("#")
        if len(f) >= 6 and f[0] == US_CITY and f[2] == "US":
            hint = "|".join([f[1], f[3], f[4], f[5]])
            if hint not in out:
                out.append(hint)
    return out


def source_id_for(record_id: str, url: str) -> str:
    """
    GKG record id plus a short URL hash. GDELT has reused one record id for
    two different articles (seen in the 2024 backfill), so the id alone is
    not unique; this keeps it traceable to the GKG record.
    """
    return f"{record_id}:{hashlib.sha1(url.encode()).hexdigest()[:8]}"


def parse_gkg_rows(rows, fetched_at: datetime) -> list[dict]:
    """GKG rows (lists of 27 fields) -> NewsItem records for US housing news."""
    records = []
    for row in rows:
        if len(row) != GKG_COLUMNS or row[COL_COLLECTION] != WEB_COLLECTION:
            continue
        themes = sorted(_HOUSING.intersection(row[COL_THEMES].split(";")))
        if not themes or "#US#" not in row[COL_LOCATIONS]:
            continue
        title = _TITLE.search(row[COL_EXTRAS])
        tone = row[COL_TONE].split(",")[0]
        cities = us_cities(row[COL_LOCATIONS])
        records.append(
            {
                "source": "gdelt",
                "source_id": source_id_for(row[COL_ID], row[COL_URL]),
                "url": row[COL_URL],
                "title": html.unescape(title.group(1)).strip() if title else None,
                "body_snippet": None,
                "published_at": datetime.strptime(
                    row[COL_DATE], "%Y%m%d%H%M%S"
                ).replace(tzinfo=timezone.utc),
                "fetched_at": fetched_at,
                "subreddit": None,
                "feed_name": row[COL_SOURCE].lower() or None,
                "geo_hint": ";".join(cities) or None,
                "engagement": None,
                "tone": float(tone) if tone else None,
                "themes": themes,
            }
        )
    return records


def read_gkg_zip(content: bytes, expected_md5: str) -> list[list[str]]:
    actual = hashlib.md5(content).hexdigest()
    if actual != expected_md5:
        raise ChecksumMismatch(f"GKG zip md5 {actual} != {expected_md5}")
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        (name,) = zf.namelist()
        text = zf.read(name).decode("utf-8", errors="replace")
    return list(csv.reader(io.StringIO(text), delimiter="\t", quoting=csv.QUOTE_NONE))


class GdeltPoller:
    def __init__(self, producer, fetcher: PoliteFetcher | None = None):
        self.producer = producer
        self.fetcher = fetcher or PoliteFetcher()
        self.seen = SeenCache()
        self.last_url: str | None = None

    def poll_once(self) -> int:
        index = self.fetcher.get(LASTUPDATE_URL)
        latest = parse_lastupdate(index.decode()) if index else None
        if latest is None or latest["url"] == self.last_url:
            logger.info("GDELT: no new GKG file")
            return 0
        content = self.fetcher.get(latest["url"])
        if content is None:
            return 0
        rows = read_gkg_zip(content, latest["md5"])
        sent = 0
        for rec in parse_gkg_rows(rows, utc_now()):
            if self.seen.add_if_new(rec["source_id"]):
                self.producer.send(rec)
                sent += 1
        self.producer.flush()
        self.last_url = latest["url"]
        logger.info(
            "GDELT %s: %d rows, sent %d housing items",
            latest["url"].rsplit("/", 1)[-1],
            len(rows),
            sent,
        )
        return sent


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    p.add_argument("--once", action="store_true", help="poll once and exit")
    args = p.parse_args(argv)
    setup_logging()
    poller = GdeltPoller(NewsProducer("gdelt"))
    if args.once:
        poller.poll_once()
    else:
        run_forever(poller.poll_once, POLL_SECONDS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
