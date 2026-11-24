"""
Weekly metro news outlook (design doc §9.4, D17):
silver.silver_news_items -> Claude (Message Batches) -> gold.metro_news_outlook.

For one Monday-aligned week:
  1. prepare: pick the top metros by scored zips (+ national), gather each
     one's news items for the week (newest first, capped) and context figures,
     and build one Messages request per metro. The requests and the inputs
     (which item each [nK] reference means) are landed in the raw zone as a
     manifest, so results can be checked against exactly what was sent.
  2. submit: one Message Batch (50% cheaper; results within 24 h).
  3. collect: read the results. Each answer must have ended normally
     (stop_reason end_turn), parse against the JSON schema, and cite input
     items: references not in the input are dropped, and an outlook with too
     few cited sentences is rejected. Failures that may succeed on a second
     try are retried once with a direct (non-batch) request; refusals are
     skipped. Nothing empty is written. The week's rows replace any earlier
     rows for the same metros and week.

News text is untrusted web content; the prompt tells the model to treat it
as data, and the citation check limits what can reach the dashboard.

    python -m llm.summarize --week-start 2026-11-16          # batch, waits
    python -m llm.summarize --week-start 2026-11-16 --direct # no batch
    python -m llm.summarize --week-start 2026-11-16 --dry-run
"""

import argparse
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

import duckdb
import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "config" / "llm.yaml"
PROMPT_DIR = REPO_ROOT / "llm" / "prompts"
NEWS_TABLE = "silver.silver_news_items"
OUTLOOK_TABLE = "gold.metro_news_outlook"
CITATIONS_TABLE = "gold.metro_news_outlook_citations"
NATIONAL = "national"
RAW_PREFIX = "llm/metro_outlook"
TONE_BASELINE_WEEKS = 12
CHARS_PER_TOKEN = 3  # conservative; Haiku 5.5's tokenizer runs ~30% above 4


# --------------------------------------------------------------------- config


@dataclass(frozen=True)
class LlmConfig:
    model: str
    effort: str
    max_tokens: int
    prompt_version: str
    top_n_metros: int
    min_items: int
    max_items: int
    snippet_chars: int
    max_prompt_tokens: int
    min_citation_coverage: float
    min_relevant_items: int

    def system_prompt(self) -> str:
        return (PROMPT_DIR / f"{self.prompt_version}.md").read_text().strip()


def load_config(path: Path | str = DEFAULT_CONFIG) -> LlmConfig:
    raw = yaml.safe_load(Path(path).read_text())
    return LlmConfig(
        model=raw["model"],
        effort=raw["effort"],
        max_tokens=int(raw["max_tokens"]),
        prompt_version=raw["prompt_version"],
        top_n_metros=int(raw["metros"]["top_n_by_scored_zips"]),
        min_items=int(raw["metros"]["min_items"]),
        max_items=int(raw["input"]["max_items"]),
        snippet_chars=int(raw["input"]["snippet_chars"]),
        max_prompt_tokens=int(raw["input"]["max_prompt_tokens"]),
        min_citation_coverage=float(raw["guardrails"]["min_citation_coverage"]),
        min_relevant_items=int(raw["guardrails"]["min_relevant_items"]),
    )


def last_complete_week(today: date) -> date:
    """Monday of the last full Monday-Sunday week before `today`."""
    return today - timedelta(days=today.weekday() + 7)


# ------------------------------------------------------------- output schema


class Outlook(BaseModel):
    """What the model returns (validated again here after the API's schema)."""

    model_config = ConfigDict(extra="forbid")

    relevant_item_ids: list[str]
    outlook: Literal["bullish", "neutral", "bearish"]
    confidence: Literal["low", "medium", "high"]
    summary: str
    key_themes: list[str]
    risks: list[str]


OUTLOOK_SCHEMA = {
    "type": "object",
    # relevant_item_ids first: the model decides what counts before writing.
    "properties": {
        "relevant_item_ids": {"type": "array", "items": {"type": "string"}},
        "outlook": {"type": "string", "enum": ["bullish", "neutral", "bearish"]},
        "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
        "summary": {"type": "string"},
        "key_themes": {"type": "array", "items": {"type": "string"}},
        "risks": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "relevant_item_ids", "outlook", "confidence", "summary", "key_themes",
        "risks",
    ],  # fmt: skip
    "additionalProperties": False,
}


# ------------------------------------------------------------------- inputs


def select_metros(con, top_n: int) -> list[dict]:
    rows = con.execute(
        """
        select cbsa_code, mode(metro) as metro_name, count(*) as scored_zips
        from gold.zip_opportunity_score
        where cbsa_code is not null
        group by cbsa_code
        order by scored_zips desc, cbsa_code
        limit ?
        """,
        [top_n],
    ).fetchall()
    return [{"cbsa_code": r[0], "metro_name": r[1], "scored_zips": r[2]} for r in rows]


def load_items(con, week_start: date, tags: list[str]) -> dict[str, list[dict]]:
    """The week's news per metro tag, newest first."""
    rows = con.execute(
        f"""
        select metro_tag, item_key, source, coalesce(feed_name, subreddit, source),
               title, body_snippet, url, published_at, tone
        from {NEWS_TABLE}
        where published_at >= ? and published_at < ?
          and metro_tag in (select unnest(?::varchar[]))
        order by metro_tag, published_at desc, item_key
        """,
        [week_start, week_start + timedelta(days=7), tags],
    ).fetchall()
    out: dict[str, list[dict]] = {t: [] for t in tags}
    for tag, key, source, outlet, title, snippet, url, published, tone in rows:
        out[tag].append(
            {
                "item_key": key,
                "source": source,
                "outlet": outlet,
                "title": title,
                "snippet": snippet,
                "url": url,
                "published_at": published.isoformat() if published else None,
                "tone": tone,
            }
        )
    return out


def _round(value, digits=2):
    return None if value is None else round(float(value), digits)


def metro_context(con, week_start: date, cbsa_codes: list[str]) -> dict[str, dict]:
    """Background figures per metro: Gold metrics and the news-tone baseline."""
    ctx = {c: {} for c in cbsa_codes}
    for code, growth, yld, jobs in con.execute(
        """
        select cbsa_code, median(rent_growth_1yr_pct), median(gross_rental_yield_pct),
               any_value(yoy_job_growth_pct)
        from gold.zip_opportunity_score
        where cbsa_code in (select unnest(?::varchar[]))
        group by cbsa_code
        """,
        [cbsa_codes],
    ).fetchall():
        ctx[code].update(
            median_rent_growth_1yr_pct=_round(growth),
            median_gross_yield_pct=_round(yld),
            job_growth_1yr_pct=_round(jobs),
        )
    for code, tone, articles in con.execute(
        """
        select cbsa_code, sum(avg_tone * article_count) / sum(article_count),
               sum(article_count)
        from silver.silver_gdelt_metro_tone
        where cbsa_code in (select unnest(?::varchar[]))
          and week_start >= ? and week_start < ?
        group by cbsa_code
        """,
        [cbsa_codes, week_start - timedelta(weeks=TONE_BASELINE_WEEKS), week_start],
    ).fetchall():
        ctx[code].update(
            news_tone_prior_12wk=_round(tone), news_articles_prior_12wk=int(articles)
        )
    return ctx


def national_context(con, week_start: date) -> dict:
    ctx = {}
    row = con.execute(
        """
        select sum(avg_tone * article_count) / sum(article_count), sum(article_count)
        from silver.silver_gdelt_national_tone
        where week_start >= ? and week_start < ?
        """,
        [week_start - timedelta(weeks=TONE_BASELINE_WEEKS), week_start],
    ).fetchone()
    if row and row[0] is not None:
        ctx.update(news_tone_prior_12wk=_round(row[0]), news_articles_prior_12wk=row[1])
    for col in (
        "mortgage_rate_30yr",
        "cpi_shelter_yoy_pct",
        "case_shiller_hpi_yoy_pct",
    ):
        found = con.execute(
            f"""
            select month, {col} from silver.silver_macro_indicators
            where {col} is not null and month < ?
            order by month desc limit 1
            """,
            [week_start + timedelta(days=7)],
        ).fetchone()
        if found:
            ctx[col] = _round(found[1])
            ctx[f"{col}_month"] = found[0].isoformat()[:7]
    return ctx


def estimate_tokens(text: str) -> int:
    return len(text) // CHARS_PER_TOKEN + 1


def render_user_prompt(
    name: str, week_start: date, context: dict, items: list[dict], snippet_chars: int
) -> str:
    week_end = week_start + timedelta(days=6)
    lines = [
        f"Metro: {name}",
        f"Week: {week_start:%Y-%m-%d} to {week_end:%Y-%m-%d}",
        "",
        "Context figures (background, not this week's news):",
    ]
    lines += [f"- {k}: {v}" for k, v in context.items()] or ["- none available"]
    lines += ["", f"News items this week ({len(items)}):", ""]
    for item in items:
        day = (item["published_at"] or "")[:10] or "undated"
        lines.append(f"[{item['ref']}] {item['outlet']} | {day} | {item['title']}")
        if item["snippet"]:
            snippet = item["snippet"][:snippet_chars].rstrip()
            lines.append(f"    {snippet}")
    return "\n".join(lines)


def build_request(cfg: LlmConfig, system_prompt: str, custom_id: str, user: str):
    return {
        "custom_id": custom_id,
        "params": {
            "model": cfg.model,
            "max_tokens": cfg.max_tokens,
            # Same system prompt for every metro: cached after the first.
            "system": [
                {
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            "messages": [{"role": "user", "content": user}],
            "output_config": {
                "effort": cfg.effort,
                "format": {"type": "json_schema", "schema": OUTLOOK_SCHEMA},
            },
        },
    }


def custom_id_for(tag: str) -> str:
    return NATIONAL if tag == NATIONAL else f"cbsa_{tag}"


def build_manifest(con, cfg: LlmConfig, week_start: date) -> dict:
    """Inputs and requests for every metro with enough news this week."""
    metros = select_metros(con, cfg.top_n_metros)
    names = {m["cbsa_code"]: m["metro_name"] for m in metros}
    names[NATIONAL] = "United States (national)"
    tags = list(names)
    items_by_tag = load_items(con, week_start, tags)
    contexts = metro_context(con, week_start, [m["cbsa_code"] for m in metros])
    contexts[NATIONAL] = national_context(con, week_start)
    system_prompt = cfg.system_prompt()

    entries, skipped = [], {}
    for tag in tags:
        all_items = items_by_tag[tag]
        if len(all_items) < cfg.min_items:
            skipped[tag] = len(all_items)
            continue
        items = [dict(it) for it in all_items[: cfg.max_items]]
        while items:
            for i, it in enumerate(items, start=1):
                it["ref"] = f"n{i}"
            user = render_user_prompt(
                names[tag], week_start, contexts[tag], items, cfg.snippet_chars
            )
            if estimate_tokens(system_prompt + user) <= cfg.max_prompt_tokens:
                break
            items = items[:-1]  # drop the oldest
        if not items:  # not even one item fits the budget
            skipped[tag] = len(all_items)
            continue
        tones = [it["tone"] for it in all_items if it["tone"] is not None]
        entries.append(
            {
                "custom_id": custom_id_for(tag),
                "cbsa_code": tag,
                "metro_name": names[tag],
                "item_count": len(all_items),
                "gdelt_avg_tone": _round(sum(tones) / len(tones)) if tones else None,
                "items": [
                    {k: it[k] for k in ("ref", "item_key", "title", "url", "source",
                                        "outlet", "published_at")}
                    for it in items
                ],  # fmt: skip
                "request": build_request(cfg, system_prompt, custom_id_for(tag), user),
            }
        )
    return {
        "week_start": week_start.isoformat(),
        "model": cfg.model,
        "prompt_version": cfg.prompt_version,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "entries": entries,
        "skipped_low_news": skipped,
    }


# ------------------------------------------------------------ checking output

REF = re.compile(r"\[(n\d+)\]")
# "... downtown. [n2][n7]" -> "... downtown [n2][n7]."
# Only when the references end the sentence: "the U.S. [n1]." stays as is.
REF_AFTER_STOP = re.compile(r"([.!?])((?:\s*\[n\d+\])+)(?=\s|$)")
# Not before a reference, so "the U.S. [n1]." is one sentence.
SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?!\[)")


class Rejected(Exception):
    def __init__(self, reason: str, retryable: bool):
        super().__init__(reason)
        self.reason = reason
        self.retryable = retryable


def check_message(message, entry: dict, cfg: LlmConfig) -> dict:
    """A finished Messages response -> an outlook row, or raise Rejected."""
    if message.stop_reason == "refusal":
        raise Rejected("refusal", retryable=False)
    if message.stop_reason != "end_turn":
        raise Rejected(f"stop_reason:{message.stop_reason}", retryable=True)
    text = next((b.text for b in message.content if b.type == "text"), None)
    if not text:
        raise Rejected("no_text", retryable=True)
    try:
        out = Outlook.model_validate_json(text)
    except ValidationError as exc:
        raise Rejected(f"invalid_json: {exc.errors()[:1]}", retryable=True) from None

    by_ref = {it["ref"]: it for it in entry["items"]}
    relevant = {r.strip("[] ") for r in out.relevant_item_ids} & set(by_ref)
    if len(relevant) < cfg.min_relevant_items:
        # Mostly off-topic news (GDELT's housing tags are broad): an outlook
        # here would only say "nothing relevant", so none is written.
        raise Rejected(f"too_little_relevant_news:{len(relevant)}", retryable=False)
    summary = REF_AFTER_STOP.sub(r"\2\1", out.summary.strip())
    sentences = [s for s in SENTENCE_END.split(summary) if s.strip()]
    cited, cited_sentences = [], 0
    for sentence in sentences:
        refs = [r for r in REF.findall(sentence) if r in by_ref]
        cited_sentences += bool(refs)
        cited += [r for r in refs if r not in cited]
    coverage = cited_sentences / len(sentences) if sentences else 0.0
    if not cited:
        raise Rejected("no_valid_citations", retryable=True)
    if coverage < cfg.min_citation_coverage:
        raise Rejected(f"citation_coverage:{coverage:.2f}", retryable=True)

    # Renumber the kept references [1], [2], ... in order of first use; drop
    # references to items that were not in the input.
    number = {ref: i for i, ref in enumerate(cited, start=1)}

    def renumber(m):
        return f"[{number[m.group(1)]}]" if m.group(1) in number else ""

    clean = re.sub(r"\s+([.!?,;:])", r"\1", REF.sub(renumber, summary))
    clean = re.sub(r"[ \t]{2,}", " ", clean).strip()
    usage = message.usage
    return {
        "outlook": out.outlook,
        "confidence": out.confidence,
        "summary": clean,
        "key_themes": [t.strip() for t in out.key_themes if t.strip()][:5],
        "risks": [r.strip() for r in out.risks if r.strip()][:3],
        "citations": [dict(by_ref[r], ref_number=number[r]) for r in cited],
        "citation_coverage": round(coverage, 3),
        "relevant_item_count": len(relevant),
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "cache_read_tokens": getattr(usage, "cache_read_input_tokens", None) or 0,
        "model_id": message.model,
    }


# ---------------------------------------------------------------- the API


def anthropic_client():
    import anthropic

    return anthropic.Anthropic()  # ANTHROPIC_API_KEY from the environment


def submit_batch(client, manifest: dict) -> str:
    requests = [e["request"] for e in manifest["entries"]]
    batch = client.messages.batches.create(requests=requests)
    logger.info("Submitted batch %s with %d requests", batch.id, len(requests))
    return batch.id


def batch_ended(client, batch_id: str) -> bool:
    batch = client.messages.batches.retrieve(batch_id)
    logger.info("Batch %s: %s %s", batch_id, batch.processing_status,
                batch.request_counts)  # fmt: skip
    return batch.processing_status == "ended"


def direct_message(client, request: dict):
    """One request outside the batch (full price; used for retries)."""
    with client.messages.stream(**request["params"]) as stream:
        return stream.get_final_message()


def collect(client, manifest: dict, batch_id: str | None, cfg: LlmConfig) -> dict:
    """Check every result, retry what may succeed once, return rows + report."""
    entries = {e["custom_id"]: e for e in manifest["entries"]}
    outcomes: dict[str, dict] = {}
    to_retry: list[str] = []

    def accept(cid, message, attempt):
        try:
            row = check_message(message, entries[cid], cfg)
        except Rejected as rej:
            outcomes[cid] = {"status": "rejected", "reason": rej.reason,
                             "attempt": attempt, **first(cid)}  # fmt: skip
            return rej.retryable
        outcomes[cid] = {"status": "ok", "row": row, "attempt": attempt, **first(cid)}
        return False

    def first(cid):
        """On a retry, keep why the first attempt failed."""
        prev = outcomes.get(cid)
        return {"first_reason": prev["reason"]} if prev else {}

    if batch_id is None:  # --direct: no batch at all
        to_retry = list(entries)
    else:
        for result in client.messages.batches.results(batch_id):
            cid, res = result.custom_id, result.result
            if res.type == "succeeded":
                if accept(cid, res.message, "batch"):
                    to_retry.append(cid)
            elif res.type == "errored" and res.error.type == "invalid_request":
                outcomes[cid] = {"status": "rejected", "reason": "invalid_request",
                                 "attempt": "batch"}  # fmt: skip
            else:  # server error, expired or canceled
                outcomes[cid] = {"status": "rejected", "reason": res.type,
                                 "attempt": "batch"}  # fmt: skip
                to_retry.append(cid)
        to_retry += [cid for cid in entries if cid not in outcomes]

    for cid in to_retry:
        attempt = "direct" if batch_id is None else "retry"
        try:
            message = direct_message(client, entries[cid]["request"])
        except Exception as exc:  # API error after the SDK's own retries
            outcomes[cid] = {"status": "rejected", "reason": f"api_error: {exc}",
                             "attempt": attempt}  # fmt: skip
            continue
        accept(cid, message, attempt)

    for cid, o in outcomes.items():
        if o["status"] != "ok":
            logger.warning("No outlook for %s: %s (%s)", cid, o["reason"], o["attempt"])
    return outcomes


# ------------------------------------------------------------------ Gold


OUTLOOK_DDL = f"""
create table if not exists {OUTLOOK_TABLE} (
    cbsa_code varchar,
    metro_name varchar,
    week_start date,
    outlook varchar,
    confidence varchar,
    summary varchar,
    key_themes varchar[],
    risks varchar[],
    cited_item_ids varchar[],
    input_item_ids varchar[],
    item_count integer,
    relevant_item_count integer,
    citation_coverage double,
    gdelt_avg_tone double,
    model_id varchar,
    prompt_version varchar,
    batch_id varchar,
    input_tokens integer,
    output_tokens integer,
    cache_read_tokens integer,
    generated_at timestamp
)
"""

CITATIONS_DDL = f"""
create table if not exists {CITATIONS_TABLE} (
    cbsa_code varchar,
    week_start date,
    ref_number integer,
    item_key varchar,
    source varchar,
    outlet varchar,
    title varchar,
    url varchar,
    published_at timestamp
)
"""


def write_gold(con, manifest: dict, outcomes: dict, batch_id: str | None) -> int:
    """Replace this week's rows for the metros that got an outlook."""
    week = date.fromisoformat(manifest["week_start"])
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    entries = {e["custom_id"]: e for e in manifest["entries"]}
    rows, cites = [], []
    for cid, o in outcomes.items():
        if o["status"] != "ok":
            continue
        e, r = entries[cid], o["row"]
        rows.append([
            e["cbsa_code"], e["metro_name"], week, r["outlook"], r["confidence"],
            r["summary"], r["key_themes"], r["risks"],
            [c["item_key"] for c in r["citations"]],
            [it["item_key"] for it in e["items"]], e["item_count"],
            r["relevant_item_count"], r["citation_coverage"], e["gdelt_avg_tone"],
            r["model_id"], manifest["prompt_version"], batch_id, r["input_tokens"],
            r["output_tokens"], r["cache_read_tokens"], now,
        ])  # fmt: skip
        cites += [
            [e["cbsa_code"], week, c["ref_number"], c["item_key"], c["source"],
             c["outlet"], c["title"], c["url"], c["published_at"]]
            for c in r["citations"]
        ]  # fmt: skip
    if not rows:
        return 0
    codes = [r[0] for r in rows]
    con.execute("create schema if not exists gold")
    con.execute(OUTLOOK_DDL)
    con.execute(CITATIONS_DDL)
    con.execute("begin")
    try:
        for table in (OUTLOOK_TABLE, CITATIONS_TABLE):
            con.execute(
                f"delete from {table} where week_start = ? "
                "and cbsa_code in (select unnest(?::varchar[]))",
                [week, codes],
            )
        con.executemany(
            f"insert into {OUTLOOK_TABLE} values ({', '.join('?' * 21)})", rows
        )
        if cites:
            con.executemany(
                f"insert into {CITATIONS_TABLE} values ({', '.join('?' * 9)})", cites
            )
        con.execute("commit")
    except Exception:
        con.execute("rollback")
        raise
    return len(rows)


def is_irrelevant(outcome: dict) -> bool:
    return outcome["reason"].startswith("too_little_relevant_news")


def report(manifest: dict, outcomes: dict) -> dict:
    ok = [o for o in outcomes.values() if o["status"] == "ok"]
    return {
        "week_start": manifest["week_start"],
        "requested": len(manifest["entries"]),
        "written": len(ok),
        "retried_ok": sum(o["attempt"] == "retry" for o in ok),
        "rejected": {
            cid: o["reason"]
            for cid, o in outcomes.items()
            if o["status"] != "ok" and not is_irrelevant(o)
        },
        "skipped_irrelevant": [
            cid
            for cid, o in outcomes.items()
            if o["status"] != "ok" and is_irrelevant(o)
        ],
        "first_attempt_failures": {cid: o["first_reason"]
                                   for cid, o in outcomes.items()
                                   if "first_reason" in o},  # fmt: skip
        "skipped_low_news": manifest["skipped_low_news"],
        "input_tokens": sum(o["row"]["input_tokens"] for o in ok),
        "output_tokens": sum(o["row"]["output_tokens"] for o in ok),
        "cache_read_tokens": sum(o["row"]["cache_read_tokens"] for o in ok),
    }


# --------------------------------------------------- raw zone (audit trail)


def land_json(key: str, payload) -> str:
    from ingestion.storage import put_raw

    body = json.dumps(payload, default=str, indent=1).encode()
    put_raw(key, body)
    return key


def read_raw_json(key: str):
    from ingestion.storage import get_s3_client, raw_bucket

    obj = get_s3_client().get_object(Bucket=raw_bucket(), Key=key)
    return json.loads(obj["Body"].read())


def manifest_key(manifest: dict) -> str:
    stamp = manifest["created_at"][:19].replace(":", "").replace("-", "")
    return f"{RAW_PREFIX}/week_start={manifest['week_start']}/{stamp}/manifest.json"


def results_key(key: str) -> str:
    """Next to the manifest; timestamped, since a retried task lands again."""
    stamp = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    return key.rsplit("/", 1)[0] + f"/results_{stamp}.json"


# ------------------------------------------------------------ entry points


def prepare(week_start: date, cfg: LlmConfig, db_path: str | None = None) -> dict:
    from scoring.score import duckdb_path

    con = duckdb.connect(db_path or duckdb_path(), read_only=True)
    try:
        return build_manifest(con, cfg, week_start)
    finally:
        con.close()


def store(manifest, outcomes, batch_id, db_path: str | None = None) -> dict:
    from scoring.score import duckdb_path

    con = duckdb.connect(db_path or duckdb_path())
    try:
        written = write_gold(con, manifest, outcomes, batch_id)
    finally:
        con.close()
    summary = report(manifest, outcomes)
    assert written == summary["written"]
    return summary


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Weekly LLM metro news outlook")
    p.add_argument(
        "--week-start",
        type=date.fromisoformat,
        help="Monday of the week. Default: last complete week.",
    )
    p.add_argument(
        "--direct",
        action="store_true",
        help="Send requests one by one instead of as a batch.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Build the requests and print a summary; no API calls.",
    )
    p.add_argument("--poll-seconds", type=int, default=60)  # fmt: skip
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    for noisy in ("httpx", "httpx2"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    from ingestion.warehouse_lock import warehouse_lock

    cfg = load_config()
    week = args.week_start or last_complete_week(datetime.now(timezone.utc).date())
    with warehouse_lock("llm_prepare"):
        manifest = prepare(week, cfg)
    sizes = {
        e["custom_id"]: (len(e["items"]), estimate_tokens(
            e["request"]["params"]["system"][0]["text"]
            + e["request"]["params"]["messages"][0]["content"]))
        for e in manifest["entries"]
    }  # fmt: skip
    print(
        json.dumps(
            {
                "week_start": manifest["week_start"],
                "items_and_tokens": sizes,
                "skipped_low_news": manifest["skipped_low_news"],
            },
            indent=1,
        )
    )
    if args.dry_run or not manifest["entries"]:
        return 0

    key = land_json(manifest_key(manifest), manifest)
    client = anthropic_client()
    batch_id = None
    if not args.direct:
        batch_id = submit_batch(client, manifest)
        while not batch_ended(client, batch_id):
            time.sleep(args.poll_seconds)
    outcomes = collect(client, manifest, batch_id, cfg)
    land_json(results_key(key), {"batch_id": batch_id, "outcomes": outcomes})
    with warehouse_lock("llm_store"):
        print("OUTLOOK_SUMMARY " + json.dumps(store(manifest, outcomes, batch_id)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
