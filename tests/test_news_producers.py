"""News producers (streaming/): parsing, Avro schema, polite fetching, polling."""

import hashlib
import io
import json
import zipfile
from datetime import datetime, timezone

import fastavro
import pytest

from streaming import common, gdelt_producer, rss_producer

FETCHED = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)

RSS = b"""<?xml version="1.0"?>
<rss version="2.0"><channel><title>Feed</title>
<item><title>Rents &amp; prices <b>cool</b></title>
  <link>https://example.com/a</link><guid>a-1</guid>
  <pubDate>Thu, 19 Nov 2026 13:00:29 +0000</pubDate>
  <description>&lt;p&gt;Landlords   offer concessions.&lt;/p&gt;</description></item>
<item><title>No link here</title></item>
<item><title>Second</title><link>https://example.com/b</link>
  <description>%s</description></item>
</channel></rss>""" % (b"x" * 2000)


def gkg_row(**overrides) -> list[str]:
    row = [""] * 27
    row[0] = "20261121061500-1"
    row[1] = "20261121061500"
    row[2] = "1"
    row[3] = "Example.com"
    row[4] = "https://example.com/story"
    row[7] = "ECON_HOUSING_PRICES;TAX_FNCACT_MAYOR"
    row[9] = (
        "1#United States#US#US#39.8#-98.5#US;"
        "3#Austin, Texas, United States#US#USTX#30.27#-97.74#1;"
        "3#Austin, Texas, United States#US#USTX#30.27#-97.74#1;"
        "4#Toronto, Ontario, Canada#CA#CA08#43.7#-79.4#2"
    )
    row[15] = "-2.5,1.0,3.5,4.5,20.0,0.5,300"
    row[26] = "<PAGE_TITLE>Austin home prices &amp; rents slip</PAGE_TITLE>"
    for idx, value in overrides.items():
        row[int(idx.lstrip("c"))] = value
    return row


@pytest.fixture(scope="module")
def schema():
    return fastavro.parse_schema(json.loads(common.SCHEMA_PATH.read_text()))


def test_schema_fields_are_backward_compatible(schema):
    """Every field added after the keys must be optional (has a default)."""
    fields = json.loads(common.SCHEMA_PATH.read_text())["fields"]
    required = [f["name"] for f in fields if "default" not in f]
    assert required == ["source", "source_id", "url", "fetched_at"]


def test_parse_feed(schema):
    recs = rss_producer.parse_feed(RSS, "test_feed", FETCHED)
    assert len(recs) == 2  # the item without a link is skipped
    a = recs[0]
    assert a["title"] == "Rents & prices cool"
    assert a["body_snippet"] == "Landlords offer concessions."
    assert a["published_at"] == datetime(2026, 11, 19, 13, 0, 29, tzinfo=timezone.utc)
    assert (
        a["source_id"]
        == rss_producer.parse_feed(RSS, "test_feed", FETCHED)[0]["source_id"]
    )  # stable across polls
    assert recs[1]["published_at"] is None
    assert len(recs[1]["body_snippet"]) == common.SNIPPET_MAX_CHARS
    for rec in recs:
        fastavro.validation.validate(rec, schema, raise_errors=True)


def test_parse_lastupdate_picks_gkg_over_https():
    text = (
        "50697 aaa http://data.gdeltproject.org/gdeltv2/20261121061500.export.CSV.zip\n"
        "2558143 bbb http://data.gdeltproject.org/gdeltv2/20261121061500.gkg.csv.zip\n"
    )
    got = gdelt_producer.parse_lastupdate(text)
    assert got == {
        "size": 2558143,
        "md5": "bbb",
        "url": "https://data.gdeltproject.org/gdeltv2/20261121061500.gkg.csv.zip",
    }


def test_parse_gkg_rows_filters_and_extracts(schema):
    rows = [
        gkg_row(),
        gkg_row(c2="2"),  # not a web article
        gkg_row(c7="TAX_FNCACT_MAYOR"),  # no housing theme
        gkg_row(c9="4#Toronto, Ontario, Canada#CA#CA08#43.7#-79.4#2"),  # not US
        gkg_row(c0="x"),  # same id reused for another url
        ["too", "short"],
    ]
    rows[4][4] = "https://other.example/story"
    rows[4][0] = rows[0][0]
    recs = gdelt_producer.parse_gkg_rows(rows, FETCHED)
    assert len(recs) == 2
    r = recs[0]
    assert r["title"] == "Austin home prices & rents slip"
    assert r["tone"] == -2.5
    assert r["themes"] == ["ECON_HOUSING_PRICES"]
    assert r["geo_hint"] == "Austin, Texas, United States|USTX|30.27|-97.74"
    assert r["feed_name"] == "example.com"
    assert r["published_at"] == datetime(2026, 11, 21, 6, 15, tzinfo=timezone.utc)
    assert recs[0]["source_id"] != recs[1]["source_id"]  # reused GKG id
    assert recs[1]["source_id"].startswith("20261121061500-1:")
    for rec in recs:
        fastavro.validation.validate(rec, schema, raise_errors=True)


def test_read_gkg_zip_checks_md5():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("x.gkg.csv", "\t".join(gkg_row()) + "\n")
    content = buf.getvalue()
    rows = gdelt_producer.read_gkg_zip(content, hashlib.md5(content).hexdigest())
    assert len(rows) == 1 and len(rows[0]) == 27
    with pytest.raises(gdelt_producer.ChecksumMismatch):
        gdelt_producer.read_gkg_zip(content, "0" * 32)


class FakeResponse:
    def __init__(self, status=200, content=b"", headers=None):
        self.status_code = status
        self.content = content
        self.text = content.decode()
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


class FakeSession:
    def __init__(self, responses):
        self.responses = responses
        self.headers = {}
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append((url, dict(headers or {})))
        return self.responses[url]


def test_polite_fetcher_honours_robots_and_etags():
    session = FakeSession(
        {
            "https://a.example/robots.txt": FakeResponse(
                content=b"User-agent: *\nDisallow: /private\n"
            ),
            "https://a.example/feed": FakeResponse(
                content=b"x", headers={"ETag": "e1"}
            ),
            "https://b.example/robots.txt": FakeResponse(status=404),
            "https://b.example/feed": FakeResponse(status=304),
        }
    )
    f = common.PoliteFetcher(session=session)
    assert session.headers["User-Agent"] == common.USER_AGENT
    assert f.get("https://a.example/private/feed") is None
    assert f.get("https://a.example/feed") == b"x"
    f.get("https://a.example/feed")
    assert session.calls[-1] == ("https://a.example/feed", {"If-None-Match": "e1"})
    assert f.get("https://b.example/feed") is None  # 304 Not Modified


def test_seen_cache_is_bounded():
    cache = common.SeenCache(max_size=2)
    assert cache.add_if_new("a") and cache.add_if_new("b")
    assert not cache.add_if_new("a")
    assert cache.add_if_new("c")  # evicts b (least recent)
    assert cache.add_if_new("b")


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, record):
        self.sent.append(record)

    def flush(self):
        pass


class FakeFetcher:
    def __init__(self, content):
        self.content = content

    def get(self, url):
        if "broken" in url:
            raise RuntimeError("boom")
        return self.content.get(url)


def test_rss_poller_sends_only_new_items_and_survives_bad_feeds():
    config = {
        "feeds": [
            {"name": "broken", "url": "https://broken.example/feed"},
            {"name": "ok", "url": "https://ok.example/feed"},
        ]
    }
    producer = FakeProducer()
    poller = rss_producer.RssPoller(
        config, producer, FakeFetcher({"https://ok.example/feed": RSS})
    )
    assert poller.poll_once() == 2
    assert poller.poll_once() == 0  # same items again
    assert {r["feed_name"] for r in producer.sent} == {"ok"}


def test_feed_config_is_valid():
    config = rss_producer.load_config()
    names = [f["name"] for f in config["feeds"]]
    assert len(names) == len(set(names)) >= 3
    assert all(f["url"].startswith("https://") for f in config["feeds"])
