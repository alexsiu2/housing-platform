"""llm/summarize.py: inputs, output checks, retries and the Gold write (no API)."""

import json
from datetime import date, datetime
from types import SimpleNamespace as NS

import duckdb
import pytest

from llm import summarize as s

WEEK = date(2026, 11, 16)
CFG = s.load_config()


def test_config_prompt_and_schema_agree():
    assert CFG.model.startswith("claude-")
    assert len(CFG.system_prompt()) > 2000  # long enough to be worth caching
    assert set(s.OUTLOOK_SCHEMA["properties"]) == set(s.Outlook.model_fields)
    assert set(s.OUTLOOK_SCHEMA["required"]) == set(s.Outlook.model_fields)


def test_last_complete_week_is_previous_monday():
    assert s.last_complete_week(date(2026, 11, 23)) == WEEK  # a Monday
    assert s.last_complete_week(date(2026, 11, 29)) == WEEK  # the Sunday after


@pytest.fixture
def con():
    c = duckdb.connect()
    for schema in ("gold", "silver"):
        c.execute(f"create schema {schema}")
    c.execute("""
        create table gold.zip_opportunity_score as
        select * from (values
            ('1', 'A', 'Metro A', 5.0, 8.0, 1.5),
            ('2', 'A', 'Metro A', 3.0, 6.0, 1.5),
            ('3', 'B', 'Metro B', 1.0, 4.0, null),
            ('4', 'C', 'Metro C', 2.0, 5.0, 0.5)
        ) t(zip, cbsa_code, metro, rent_growth_1yr_pct, gross_rental_yield_pct,
            yoy_job_growth_pct)
        """)
    c.execute("""
        create table silver.silver_gdelt_metro_tone as
        select 'A' as cbsa_code, date '2026-11-09' as week_start,
               10 as article_count, -1.5 as avg_tone
        """)
    c.execute("""
        create table silver.silver_gdelt_national_tone as
        select date '2026-11-09' as week_start, 100 as article_count, -0.5 as avg_tone
        """)
    c.execute("""
        create table silver.silver_macro_indicators as
        select date '2026-10-01' as month, 6.25 as mortgage_rate_30yr,
               3.1 as cpi_shelter_yoy_pct, null::double as case_shiller_hpi_yoy_pct
        """)
    rows = []
    for i in range(6):  # Metro A: 6 items this week, one last week
        rows.append((f"a{i}", "A", "gdelt", f"Metro A story {i}", "snippet " * 20,
                     datetime(2026, 11, 16 + i, 12), -2.0))  # fmt: skip
    rows.append(
        ("old", "A", "rss", "Last week", None, datetime(2026, 11, 15, 23), None)
    )
    rows += [("b0", "B", "rss", "Only one", None, datetime(2026, 11, 17), None)]
    rows += [(f"n{i}", "national", "rss", f"US story {i}", None,
              datetime(2026, 11, 18), None) for i in range(3)]  # fmt: skip
    c.execute("""
        create table silver.silver_news_items (
            item_key varchar, metro_tag varchar, source varchar, title varchar,
            body_snippet varchar, published_at timestamp, tone double,
            feed_name varchar, subreddit varchar, url varchar)
        """)
    c.executemany(
        "insert into silver.silver_news_items values (?, ?, ?, ?, ?, ?, ?, "
        "'outlet', null, 'https://x/' || ?)",
        [r + (r[0],) for r in rows],
    )
    yield c
    c.close()


def test_manifest_picks_metros_with_enough_news(con):
    m = s.build_manifest(con, CFG, WEEK)
    ids = [e["custom_id"] for e in m["entries"]]
    assert ids == ["cbsa_A", "national"]  # B has 1 item, C none
    assert m["skipped_low_news"] == {"B": 1, "C": 0}
    a = m["entries"][0]
    assert a["item_count"] == 6 and a["gdelt_avg_tone"] == -2.0
    assert [it["ref"] for it in a["items"]] == [f"n{i}" for i in range(1, 7)]
    assert a["items"][0]["item_key"] == "a5"  # newest first
    user = a["request"]["params"]["messages"][0]["content"]
    assert "Metro: Metro A" in user and "[n1] outlet | 2026-11-21 |" in user
    assert "news_tone_prior_12wk: -1.5" in user  # last week's GDELT tone
    assert "Last week" not in user  # outside the window
    national = m["entries"][1]["request"]["params"]["messages"][0]["content"]
    assert "mortgage_rate_30yr: 6.25" in national
    params = a["request"]["params"]
    assert params["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert params["output_config"]["format"]["schema"] == s.OUTLOOK_SCHEMA
    json.dumps(m)  # lands in the raw zone as JSON


def test_manifest_trims_items_to_the_token_budget(con):
    budget = s.estimate_tokens(CFG.system_prompt()) + 250
    small = s.LlmConfig(**{**CFG.__dict__, "max_prompt_tokens": budget})
    m = s.build_manifest(con, small, WEEK)
    a = m["entries"][0]
    assert 0 < len(a["items"]) < 6 and a["item_count"] == 6
    text = a["request"]["params"]["system"][0]["text"]
    text += a["request"]["params"]["messages"][0]["content"]
    assert s.estimate_tokens(text) <= budget
    # Nothing fits: skipped, not sent empty.
    tiny = s.LlmConfig(**{**CFG.__dict__, "max_prompt_tokens": 10})
    assert s.build_manifest(con, tiny, WEEK)["entries"] == []


ENTRY = {
    "custom_id": "cbsa_A",
    "items": [
        {"ref": f"n{i}", "item_key": f"k{i}", "title": f"t{i}", "url": f"u{i}",
         "source": "rss", "outlet": "o", "published_at": "2026-11-17T00:00:00"}
        for i in range(1, 4)
    ],
}  # fmt: skip


def message(summary=None, stop="end_turn", raw=None, relevant=("n1", "[n2]")):
    body = raw if raw is not None else json.dumps({
        "relevant_item_ids": relevant, "outlook": "bearish",
        "confidence": "medium", "summary": summary,
        "key_themes": ["insurance costs", " "], "risks": ["higher premiums"],
    })  # fmt: skip
    return NS(
        stop_reason=stop,
        content=[NS(type="thinking"), NS(type="text", text=body)],
        usage=NS(input_tokens=1000, output_tokens=200, cache_read_input_tokens=800),
        model="claude-haiku-5-5",
    )


def test_check_message_handles_abbreviations_before_citations():
    row = s.check_message(
        message("Homes listed in the U.S. [n1]. Rents rose. [n2] Done [n2]."),
        ENTRY,
        CFG,
    )
    assert row["summary"] == "Homes listed in the U.S. [1]. Rents rose [2]. Done [2]."
    assert row["citation_coverage"] == 1.0


def test_check_message_renumbers_and_drops_unknown_citations():
    row = s.check_message(
        message("Premiums rose [n3]. Two insurers left [n9][n1]. Rents held. [n3]"),
        ENTRY,
        CFG,
    )
    assert row["summary"] == "Premiums rose [1]. Two insurers left [2]. Rents held [1]."
    assert [c["item_key"] for c in row["citations"]] == ["k3", "k1"]
    assert [c["ref_number"] for c in row["citations"]] == [1, 2]
    assert row["citation_coverage"] == 1.0
    assert row["key_themes"] == ["insurance costs"]
    assert row["cache_read_tokens"] == 800
    assert row["relevant_item_count"] == 2  # "[n2]" is tolerated


@pytest.mark.parametrize(
    "msg, reason, retryable",
    [
        (message("x [n1].", stop="refusal"), "refusal", False),
        (message("x [n1].", stop="max_tokens"), "stop_reason:max_tokens", True),
        (message(raw='{"outlook": "up"}'), "invalid_json", True),
        (message("No sources here. None at all [n7]."), "no_valid_citations", True),
        (message("One [n1]. Two. Three."), "citation_coverage:0.33", True),
        # n9 isn't an input item, so only one relevant item: no outlook, no retry.
        (
            message("x [n1].", relevant=["n1", "n9"]),
            "too_little_relevant_news:1",
            False,
        ),
    ],
)
def test_check_message_rejections(msg, reason, retryable):
    with pytest.raises(s.Rejected) as exc:
        s.check_message(msg, ENTRY, CFG)
    assert exc.value.reason.startswith(reason)
    assert exc.value.retryable is retryable


class FakeBatches:
    def __init__(self, results):
        self._results = results

    def results(self, batch_id):
        return iter(self._results)


class FakeClient:
    """Batch results as given; direct requests answer from `direct`."""

    def __init__(self, results, direct):
        self.messages = NS(batches=FakeBatches(results), stream=self._stream)
        self.direct = direct
        self.direct_calls = []

    def _stream(self, **params):
        cid = params["messages"][0]["content"]
        self.direct_calls.append(cid)
        msg = self.direct[cid]

        class Ctx:
            def __enter__(self_):
                return NS(get_final_message=lambda: msg)

            def __exit__(self_, *a):
                return False

        return Ctx()


def entry(cid):
    return {**ENTRY, "custom_id": cid, "cbsa_code": cid, "metro_name": cid,
            "item_count": 3, "gdelt_avg_tone": None,
            "request": {"params": {"messages": [{"content": cid}]}}}  # fmt: skip


def test_collect_retries_what_may_succeed_once():
    good = message("Rents fell [n1].")
    manifest = {"entries": [entry(c) for c in ("ok", "weak", "bad", "gone", "no")]}
    results = [
        NS(custom_id="ok", result=NS(type="succeeded", message=good)),
        NS(custom_id="weak", result=NS(type="succeeded", message=message("A. B."))),
        NS(custom_id="bad", result=NS(type="errored",
                                      error=NS(type="invalid_request"))),
        NS(custom_id="gone", result=NS(type="expired")),
        NS(custom_id="no", result=NS(type="succeeded",
                                     message=message("x [n1].", stop="refusal"))),
    ]  # fmt: skip
    client = FakeClient(results, direct={"weak": good, "gone": message("Z.")})
    out = s.collect(client, manifest, "batch_1", CFG)
    assert sorted(client.direct_calls) == ["gone", "weak"]
    assert out["ok"]["status"] == "ok" and out["ok"]["attempt"] == "batch"
    assert out["weak"]["status"] == "ok" and out["weak"]["attempt"] == "retry"
    assert out["bad"]["reason"] == "invalid_request"
    assert out["gone"]["reason"] == "no_valid_citations"  # retried, still failed
    assert out["gone"]["first_reason"] == "expired"
    assert out["weak"]["first_reason"].startswith("no_valid_citations")
    assert out["no"]["reason"] == "refusal"


def test_write_gold_replaces_only_this_week(tmp_path):
    con = duckdb.connect(str(tmp_path / "wh.duckdb"))
    manifest = {
        "week_start": "2026-11-16",
        "prompt_version": "v",
        "entries": [entry("A"), entry("B")],
        "skipped_low_news": {},
    }
    ok = {"status": "ok", "attempt": "batch",
          "row": s.check_message(message("Rents fell [n2]."), ENTRY, CFG)}  # fmt: skip
    assert s.write_gold(con, manifest, {"A": ok, "B": ok}, "b1") == 2
    later = {**manifest, "week_start": "2026-11-23"}
    s.write_gold(con, later, {"A": ok}, "b2")
    rejected = {"status": "rejected", "reason": "refusal", "attempt": "batch"}
    assert s.write_gold(con, manifest, {"A": ok, "B": rejected}, "b3") == 1
    rows = con.execute(
        f"select cbsa_code, week_start, batch_id, cited_item_ids, input_item_ids "
        f"from {s.OUTLOOK_TABLE} order by all"
    ).fetchall()
    assert [(r[0], str(r[1]), r[2]) for r in rows] == [
        ("A", "2026-11-16", "b3"),  # replaced
        ("A", "2026-11-23", "b2"),  # other week untouched
        ("B", "2026-11-16", "b1"),  # rejected rerun keeps the earlier outlook
    ]
    assert rows[0][3] == ["k2"] and rows[0][4] == ["k1", "k2", "k3"]
    cites = con.execute(f"select count(*) from {s.CITATIONS_TABLE}").fetchone()[0]
    assert cites == 3
    report = s.report(manifest, {"A": ok, "B": rejected})
    assert report["written"] == 1 and report["rejected"] == {"B": "refusal"}
