# Rental Property Opportunity Finder
## Technical Design Document v3.0

*Status: Draft · Last revised: 2026-11-11 · Author: Alex (UC Berkeley, MIDS/Haas)*

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [What Changed from v2.0](#2-what-changed-from-v20)
3. [Goals, Non-Goals & Success Metrics](#3-goals-non-goals--success-metrics)
4. [Architecture Overview](#4-architecture-overview)
5. [Data Sources & Volumes](#5-data-sources--volumes)
6. [Storage & Data Model](#6-storage--data-model)
7. [Opportunity Score Design](#7-opportunity-score-design)
8. [Batch Pipeline Design](#8-batch-pipeline-design)
9. [Streaming News & Sentiment Pipeline](#9-streaming-news--sentiment-pipeline)
10. [Data Quality, Lineage & Observability](#10-data-quality-lineage--observability)
11. [Local Platform (Docker Compose)](#11-local-platform-docker-compose)
12. [Cloud Deployment & IAM (Deferred)](#12-cloud-deployment--iam-deferred)
13. [Repo Structure](#13-repo-structure)
14. [Build Plan — Phases](#14-build-plan--phases)
15. [Cost Model](#15-cost-model)
16. [Tech Stack & Decision Log](#16-tech-stack--decision-log)
17. [Risks & Mitigations](#17-risks--mitigations)
18. [Appendix: Repo Cleanup Backlog](#18-appendix-repo-cleanup-backlog)

---

## 1. Project Overview

**Project Name:** Rental Property Opportunity Finder (RPOF)

**One-Line Summary:** A local-first data platform that ingests free, open housing, lending, labor and news data through both batch (Airflow + Spark) and streaming (Kafka + Spark Structured Streaming) paths, models it in a Bronze → Silver → Gold lakehouse with dbt, and serves a ranked, explainable zip-code opportunity score plus an AI-summarized market outlook for rental property investors.

**The core question the platform answers:**
> "Which US zip codes right now offer the best combination of rent growth, affordable entry price, sustainable renter demand and job growth — and what is the market saying about them?"

**Why this project:**
- **Personally useful** — a real tool for evaluating rental investments, not a toy.
- **Portfolio signal** — demonstrates the tools data engineering teams use (Airflow, Spark, Kafka, dbt, Delta Lake, Terraform, CI/CD, data quality, lineage) where each tool has a *real* job, justified by the data.
- **$0 data cost** — every data source is free/open.
- **Low running cost** — the whole stack runs locally in Docker. The only recurring outside cost is the LLM summarization step (~$1–6/month). A cloud deployment is designed but deferred.

**Guiding principle — every tool earns its place.** The core housing data (~10M rows, monthly cadence) fits on a laptop. Spark is introduced for genuinely large sources (HMDA loan-level records, GDELT), and Kafka for genuinely continuous ones (news/Reddit). No tool is added only for the résumé.

---

## 2. What Changed from v2.0

| Area | v2.0 | v3.0 | Why |
|---|---|---|---|
| Hosting | Local dev → GCP + Snowflake | **Local-first**; cloud deferred to optional phase | Keep cost near $0 while building |
| Cloud warehouse | Snowflake | **BigQuery** (when deployed) | Free tier covers this scale; already on GCP |
| Spark | Not used | **Spark batch** for HMDA + GDELT backfills; **Structured Streaming** for news | New large sources justify it |
| Kafka | Not used | **Kafka (KRaft) + Schema Registry** for news/Reddit/GDELT events | Continuous event data justifies it |
| New sources | — | HMDA, GDELT, Reddit, RSS news, Redfin (stretch) | Investor activity + market sentiment |
| AI layer | — | **Weekly LLM market outlook per metro** (Claude, Batch API) | Turns unstructured text into a Gold table |
| Jobs data | BLS QCEW (quarterly, ~5–6 mo lag) | **BLS CES metro employment** (monthly) | More timely; QCEW kept as optional |
| IaC / IAM | "Terraform as stretch" | **Terraform** for IAM, buckets, BigQuery, budget alerts (cloud phase) | Least-privilege IAM is a core skill |
| Data quality | dbt tests | dbt tests + **Great Expectations** at ingestion | Catch bad data before Bronze |
| Lineage / monitoring | — | **OpenLineage + Marquez**, **Prometheus + Grafana** | Production-maturity signals |
| CI/CD | Stretch | **GitHub Actions** from Phase 0 | Tests and lint on every PR |
| Databricks | — | **Omitted** (possible future stretch via Free Edition) | Avoids cost and platform split |
| Unused deps | MLflow, XGBoost, scikit-learn, RapidAPI, Walk Score | **Removed** | Leftovers from v1.0 scope |

---

## 3. Goals, Non-Goals & Success Metrics

### 3.1 Primary Goals

| Goal | Definition of Done |
|---|---|
| Core batch ingest (Zillow, FRED, Census, BLS, HUD) | Each source lands in Bronze Delta on a schedule via Airflow; re-runs are idempotent |
| Lakehouse modeling | Silver + Gold dbt models build with all tests passing |
| Opportunity score | `gold.zip_opportunity_score` refreshed monthly, fully decomposable into sub-scores |
| Large-scale batch with Spark | HMDA loan-level history (2018+) processed by Spark into zip-level investor-activity metrics |
| Streaming ingest | RSS, Reddit and GDELT events flow through Kafka → Spark Structured Streaming → Bronze Delta continuously |
| AI market outlook | `gold.metro_news_outlook` refreshed weekly with structured, source-cited summaries |
| Dashboard | Evidence.dev site with 5 pages runs locally and builds to static files |
| Data quality | GE checks at ingestion + dbt tests on all Silver/Gold models; failures alert, never pass silently |
| Lineage & monitoring | Every Airflow/Spark/dbt job emits OpenLineage events to Marquez; Grafana shows freshness + Kafka lag |
| CI | GitHub Actions runs lint, unit tests and `dbt build` (against DuckDB) on every PR |
| Reproducibility | `make up` brings up the entire stack on a fresh machine |

### 3.2 Non-Goals (for now)

- Property-level valuation or listing scraping (zip/metro granularity only).
- ML price prediction (removed in v2.0; may return later).
- Always-on cloud infrastructure or managed services with fixed monthly fees.
- Databricks (see Decision Log).

### 3.3 Stretch Goals

| Goal | Notes |
|---|---|
| Cloud deployment (Phase 8) | Terraform-managed GCP: GCS, BigQuery, IAM, Secret Manager, budget alerts |
| Redfin market data | Inventory, days on market, sale-to-list by zip |
| HUD Fair Market Rents | Second rent source for cross-validation |
| Sentiment as a score input | Add `score_market_sentiment` once outlook data has a few months of history |
| Tier-change alerts | Email/Slack when a zip's tier changes |
| Historical score tracking | Snapshot scores monthly (dbt snapshots) |
| Databricks Free Edition | Run the HMDA Spark job there as a portability demo |

---

## 4. Architecture Overview

```
                        ┌──────────────────────────────── BATCH PATH ────────────────────────────────┐
                        │                                                                             │
  Zillow CSVs ─┐        │  Airflow DAGs                                                               │
  FRED API    ─┤        │   ingest_* tasks (Python clients) ──► raw files ──► GE validation ──┐       │
  Census API  ─┼────────┼─►                                     (MinIO)                      │       │
  BLS API     ─┤        │                                                                    ▼       │
  HUD xwalk   ─┘        │                                                    BRONZE (Delta on MinIO) │
                        │                                                                    ▲       │
  HMDA files  ─┐        │   spark_* tasks (Spark batch, spark-submit) ───────────────────────┘       │
  GDELT bulk  ─┘────────┼─►  heavy parsing / filtering / aggregation                                 │
                        └─────────────────────────────────────────────────────────────────────────────┘

                        ┌────────────────────────────── STREAMING PATH ──────────────────────────────┐
  RSS feeds   ─┐        │  Producers (Python,      Kafka (KRaft)              Spark Structured        │
  Reddit API  ─┼────────┼─► polling services) ──►  + Schema Registry  ──────► Streaming ──► BRONZE   │
  GDELT 15min ─┘        │                          topics: news.*             (dedupe, parse)  Delta  │
                        └─────────────────────────────────────────────────────────────────────────────┘

                                                         │
                                                         ▼
                        ┌───────────────────────── TRANSFORM & SERVE ────────────────────────────────┐
                        │  dbt (dbt-duckdb locally / dbt-bigquery in cloud)                          │
                        │    staging → SILVER → GOLD                                                 │
                        │  scoring/score.py ──► gold.zip_opportunity_score                           │
                        │  llm/summarize.py (Claude Batch API, weekly) ──► gold.metro_news_outlook   │
                        │                                                                            │
                        │  DuckDB (local)  ──►  Evidence.dev dashboard (static build)                │
                        └─────────────────────────────────────────────────────────────────────────────┘

  Cross-cutting:  Great Expectations · dbt tests · OpenLineage → Marquez · Prometheus → Grafana
                  GitHub Actions CI · Terraform (cloud phase) · Docker Compose (everything local)
```

**Division of labor between tools:**

| Tool | Job | Not its job |
|---|---|---|
| Airflow | Scheduling, dependencies, retries, backfills for batch work | Moving data itself; streaming |
| Python clients | Small API/CSV pulls (FRED, Census, BLS, Zillow) | Heavy transforms |
| Spark (batch) | Large files: HMDA (tens of millions of rows/yr), GDELT history | Small API pulls |
| Kafka | Durable, replayable buffer for continuous events | Long-term storage (Delta is the source of truth) |
| Spark Structured Streaming | Kafka → Bronze Delta, dedupe, light parsing | Business logic |
| dbt | All business modeling Silver/Gold, tests, docs | Raw ingestion |
| DuckDB | Local warehouse / query engine over Delta | — |
| Evidence | Dashboard | — |

---

## 5. Data Sources & Volumes

### 5.1 Summary

| Source | Grain | Approx. volume | Cadence | Auth | Ingest via |
|---|---|---|---|---|---|
| Zillow ZHVI (home values) | zip × month | ~26k zips × ~310 mo ≈ **8M rows** | Monthly | None | Python + Airflow |
| Zillow ZORI (rents) | zip × month | several thousand zips × ~130 mo ≈ **<1M rows** | Monthly | None | Python + Airflow |
| FRED macro | series × date | **<10k rows** | Weekly | Free API key | Python + Airflow |
| Census ACS 5-yr | ZCTA × year | ~33k × years ≈ **~150k rows** | Annual (Dec) | Free API key | Python + Airflow |
| BLS CES metro employment | metro × month | ~400 metros × months ≈ **~100k rows** | Monthly | Free API key | Python + Airflow |
| HUD USPS crosswalks | zip↔CBSA, tract↔zip | **~100k rows** | Quarterly | Free account | Python + Airflow (dbt seed for a pinned copy) |
| **HMDA** loan-level | loan application | **~10–25M rows/yr, several GB/yr**; 2018+ ≈ 100M+ rows | Annual (+ quarterly for large filers) | None | **Spark batch** |
| **GDELT** GKG / events | article/event | Billions of rows total; filtered to US housing themes | Every 15 min | None | **Spark batch** (history) + **Kafka** (live) |
| **Reddit** | post/comment | ~1–10k posts/week across target subreddits | Continuous (polled) | Free OAuth app | **Kafka** producer |
| **RSS news** | article (headline, snippet, link) | ~1–5k items/week | Continuous (polled) | None | **Kafka** producer |
| Redfin (stretch) | zip × month/week | ~GBs | Weekly/monthly | None | Spark or Python |

**Totals:** core housing data ~10M rows (<1 GB). With HMDA + GDELT the platform reaches **100M+ rows / tens of GB**, which is where Spark is justified.

### 5.2 Source Notes

**Zillow ZORI / ZHVI** — Direct CSV downloads from Zillow Research. Wide format (one column per month) → melted to long in ingestion. Verify current file names and zip coverage on first download; record them in `config/sources.yaml`.

**FRED** — Already implemented in `ingestion/fred_client.py`. Final series list (reconciles code and v2 doc):

| Series | Meaning | Use |
|---|---|---|
| `MORTGAGE30US` | 30-yr fixed mortgage rate | Investor financing cost |
| `CUUR0000SAH` | CPI: Shelter | National rent inflation benchmark |
| `HOUST` | Housing starts | Supply signal |
| `UNRATE` | Unemployment rate | Macro health |
| `CSUSHPISA` | Case-Shiller national HPI | Price context |
| `MSPUS` | Median sale price of houses sold | Price context |

**Census ACS (5-year, ZCTA)** — `B19013_001E` median household income, `B01003_001E` population, `B25003_001E` occupied units, `B25003_003E` renter-occupied units, `B23025_005E` unemployed, `B25064_001E` median gross rent (cross-check vs ZORI). Note ZCTAs ≈ zips but are not identical — documented as a known approximation.

**BLS CES (State & Metro Area Employment)** — Monthly total nonfarm employment by metro via the BLS Public API v2 (free registration key; daily query and series-per-query limits apply, so batch series IDs per request). Replaces QCEW as primary because it's monthly and more timely; QCEW remains an optional cross-check.

**HUD USPS crosswalks** — Two files: ZIP→CBSA (connects zip data to metro data) and TRACT→ZIP (needed because **HMDA is reported at census-tract level, not zip**). Use the `res_ratio` (residential address share) to allocate tract metrics to zips.

**HMDA (CFPB)** — Every US mortgage application. Key fields: `census_tract`, `action_taken`, `loan_purpose`, `occupancy_type` (3 = investment property), `loan_amount`, `property_value`, `derived_dwelling_category`, `activity_year`. Produces **investor purchase share**, loan volume trends and median loan size by zip — strong signals of investor competition.

**GDELT** — Global news event and Global Knowledge Graph (GKG) data, updated every 15 minutes, history back to 2015. Filter to US locations and housing-related GKG themes (e.g. housing prices, rent, eviction). Gives a high-volume, structured news signal with geographic coordinates.

**Reddit** — Official Reddit API via PRAW (OAuth app, free tier has rate limits; review the current Data API terms before building — non-commercial personal project use). Target subreddits: r/realestateinvesting, r/landlord, r/RealEstate, r/FirstTimeHomeBuyer, plus city subreddits for tracked metros (mapping in `config/subreddit_metro_map.yaml`). Store post ID, subreddit, title, body, score, created time, permalink — not usernames.

**RSS news** — Ingest headline, snippet, publish date and link from feeds (housing sections of news outlets, Zillow/Redfin/Realtor.com research blogs, Google News RSS queries per metro). **Do not scrape full article bodies from sites whose terms forbid it or that are paywalled**; the LLM works from headlines/snippets plus Reddit text and GDELT metadata. Respect `robots.txt` and set a descriptive user agent.

---

## 6. Storage & Data Model

### 6.1 Storage Layout

```
MinIO (local S3)  ── later: GCS
  s3://rpof-raw/                      # immutable landing zone (files exactly as downloaded)
    zillow/{zori|zhvi}/YYYY/MM/*.csv
    fred/YYYY/MM/DD/*.json
    census/acs5/YYYY/*.json
    bls/ces/YYYY/MM/*.json
    hud/crosswalk/YYYY_QN/*.xlsx
    hmda/YYYY/*.csv.gz
    gdelt/gkg/YYYY/MM/DD/*.zip
  s3://rpof-lake/                     # Delta tables
    bronze/<table>/
    silver_spark/<table>/              # Spark-produced aggregates (HMDA, GDELT)
  ./data/rpof.duckdb                  # dbt Silver/Gold models (local warehouse)
```

- **Bronze** = Delta tables on MinIO, append/merge only, schema-on-read, with `ingested_at`, `source_file`, `run_id` on every row.
- **Silver/Gold** = dbt models. Locally materialized in DuckDB, reading Bronze via DuckDB's `delta_scan` over S3/MinIO. In the cloud phase the same models run on BigQuery.
- Spark writes heavy aggregates (HMDA, GDELT) to `silver_spark/` Delta tables, which dbt treats as sources.

### 6.2 Bronze Tables

| Table | Key | Notes |
|---|---|---|
| `bronze.zori_raw` | (region_name, month) | melted long format |
| `bronze.zhvi_raw` | (region_name, month) | melted long format |
| `bronze.fred_series_raw` | (series_id, date) | already shaped by `fred_client.py` |
| `bronze.census_acs_raw` | (zcta, variable, vintage_year) | long format |
| `bronze.bls_ces_raw` | (series_id, period) | |
| `bronze.hud_crosswalk_raw` | (zip, cbsa / tract, vintage) | |
| `bronze.hmda_lar_raw` | (activity_year, row hash) | partitioned by `activity_year`, `state_code` |
| `bronze.gdelt_gkg_raw` | (gkg_record_id) | partitioned by date; pre-filtered to US |
| `bronze.news_events_raw` | (source, source_id) | from Kafka; RSS + Reddit + live GDELT |

### 6.3 Silver Models (dbt unless noted)

| Model | Grain | Built by |
|---|---|---|
| `silver_rent_index` | zip × month | dbt |
| `silver_home_values` | zip × month (+ cbsa_code) | dbt |
| `silver_macro_indicators` | date (pivoted FRED, forward-filled to daily/monthly) | dbt |
| `silver_zip_demographics` | zip × vintage_year | dbt |
| `silver_metro_job_growth` | cbsa × month | dbt |
| `silver_zip_cbsa_xwalk` / `silver_tract_zip_xwalk` | mapping | dbt |
| `silver_zip_lending_activity` | zip × year (investor share, loan counts, median loan) | **Spark** → dbt source |
| `silver_news_items` | item (deduped, metro-tagged, language-filtered) | dbt over Bronze |
| `silver_gdelt_metro_tone` | cbsa × week (article count, avg tone) | **Spark** → dbt source |

### 6.4 Gold Models

**`gold_zip_investment_metrics`** — one row per zip per month (logic as in v2.0 §7.3), extended with:

```sql
    -- Lending / investor activity (HMDA, annual, joined on latest available year)
    investor_purchase_share     FLOAT,   -- % of home-purchase originations with occupancy_type = investment
    purchase_loan_count_yoy     FLOAT,   -- % change in purchase originations
    -- Freshness
    lending_data_vintage        INT
```

**`gold_zip_opportunity_score`** — one row per zip (latest month); schema unchanged from v2.0 plus optional `score_investor_competition` (see §7.5).

**`gold_metro_news_outlook`** — one row per metro per week:

```sql
CREATE TABLE gold.metro_news_outlook (
    cbsa_code           STRING,
    metro_name          STRING,
    week_start          DATE,
    outlook             STRING,      -- 'bullish' | 'neutral' | 'bearish'
    confidence          STRING,      -- 'low' | 'medium' | 'high'
    summary             STRING,      -- 3–5 sentence plain-English outlook
    key_themes          ARRAY<STRING>,
    risks               ARRAY<STRING>,
    cited_item_ids      ARRAY<STRING>,  -- FK → silver_news_items, for traceability
    item_count          INT,
    gdelt_avg_tone      FLOAT,       -- quantitative cross-check
    model_id            STRING,      -- which LLM produced it
    prompt_version      STRING,
    generated_at        TIMESTAMP
);
```

---

## 7. Opportunity Score Design

The score must be **explainable**: every zip's score decomposes into sub-scores, each tracing back to one raw metric.

### 7.1 Sub-Scores (v1 — ships in Phase 3)

| Sub-Score | Raw Metric | Direction | Weight |
|---|---|---|---|
| `score_rental_yield` | `gross_rental_yield` | higher = better | 30% |
| `score_rent_growth` | `rent_growth_1yr` | higher = better | 25% |
| `score_affordability` | `rent_to_income_ratio` | lower = better | 20% |
| `score_entry_price` | `price_to_rent_ratio` | lower = better | 15% |
| `score_demand_growth` | `yoy_job_growth_pct` (metro) | higher = better | 10% |

### 7.2 Normalization

Percentile rank within the current snapshot month, scaled to 0–100 (robust to outliers; "80" = beats 80% of zips). Inverted metrics use `(1 - pct_rank) * 100`.

### 7.3 Composite & Tiers

```python
opportunity_score = (
    0.30 * score_rental_yield
  + 0.25 * score_rent_growth
  + 0.20 * score_affordability
  + 0.15 * score_entry_price
  + 0.10 * score_demand_growth
)
tier = "strong" if score >= 70 else "moderate" if score >= 45 else "weak"
```

Weights live in `config/scoring.yaml` (not hard-coded) so they are versioned and can be tuned without code changes. Each scoring run records `weights_version`.

### 7.4 Missing Data Rule

A zip is scored only if yield, rent growth and affordability are all present. If `yoy_job_growth_pct` is missing (zip not in a CBSA), its weight is redistributed proportionally and the row is flagged `partial_score = true`.

### 7.5 Score v2 Candidates (after Phases 5–6)

| Candidate | Source | Direction | Notes |
|---|---|---|---|
| `score_investor_competition` | HMDA investor share | **lower** = better (less competition) | Debatable direction — high share can also signal a proven rental market. Evaluate before adding. |
| `score_market_sentiment` | `gold_metro_news_outlook` + GDELT tone | more positive = better | Add only after ≥3 months of history; cap weight at 5–10%. |

Any v2 change ships as a new `weights_version` with a before/after comparison of the top 50.

---

## 8. Batch Pipeline Design

### 8.1 Airflow DAGs

| DAG | Schedule | Tasks |
|---|---|---|
| `ingest_fred` | Weekly (Mon) | fetch → land raw JSON → GE validate → merge to Bronze |
| `ingest_zillow` | Monthly (day 10) | download ZORI/ZHVI → land → melt → GE → merge |
| `ingest_census_acs` | Annual (Jan) + manual | fetch variables by ZCTA → land → GE → merge |
| `ingest_bls_ces` | Monthly | fetch metro series → land → GE → merge |
| `ingest_hud_crosswalk` | Quarterly | download → land → merge |
| `spark_hmda` | Annual + manual backfill | download year files → `spark-submit jobs/hmda_lar.py` → Bronze + `silver_spark.zip_lending_activity` |
| `spark_gdelt_backfill` | Manual / monthly | Spark job filters GKG history → `silver_spark.gdelt_metro_tone` |
| `dbt_build` | Triggered by upstream datasets | `dbt build --select state:modified+` (or by tag) |
| `compute_scores` | After `dbt_build` (monthly) | `scoring/score.py` → Gold |
| `llm_metro_outlook` | Weekly (Sun) | see §9.4 |
| `data_quality_report` | Daily | freshness checks, GE summary, alert on failure |

Use **Airflow Datasets/Assets** (data-aware scheduling) so `dbt_build` runs when its upstream Bronze tables update, instead of fixed-time chaining.

### 8.2 Ingestion Pattern (all Python clients)

```
1. fetch        → API/CSV → bytes
2. land_raw     → s3://rpof-raw/<source>/<partition>/<file>   (immutable, never overwritten)
3. validate     → Great Expectations suite for the source
4. load_bronze  → deltalake MERGE on natural key (idempotent; safe to re-run/backfill)
5. audit        → row counts, run_id, duration → meta.ingest_audit
```

Clients take `start_date`/`end_date` or a partition key so Airflow backfills work naturally. Secrets come from `.env` locally (Secret Manager in cloud).

### 8.3 Spark Batch Jobs

**`jobs/hmda_lar.py`**
1. Read yearly LAR CSVs from `rpof-raw/hmda/` with an explicit schema.
2. Filter: `action_taken = 1` (originated), `loan_purpose = 1` (home purchase), 1–4 unit dwellings.
3. Write `bronze.hmda_lar_raw` (Delta, partitioned by `activity_year`, `state_code`).
4. Aggregate tract × year: total purchases, investor purchases (`occupancy_type = 3`), median loan amount.
5. Join tract→zip crosswalk, allocate by `res_ratio`, write `silver_spark.zip_lending_activity`.

**`jobs/gdelt_gkg_filter.py`**
1. Read GKG files for a date range.
2. Filter to US locations + housing themes; extract location → CBSA (lat/long → county FIPS → CBSA via crosswalk).
3. Aggregate cbsa × week: article count, average tone, top themes → `silver_spark.gdelt_metro_tone`.

Spark runs locally in Docker (master + 1–2 workers) with `delta-spark`; Airflow submits via `SparkSubmitOperator`. Jobs are written to be portable (no local-path assumptions) so they could later run on Dataproc Serverless or Databricks unchanged.

---

## 9. Streaming News & Sentiment Pipeline

### 9.1 Flow

```
Producers (Python, one container each)          Kafka topics (Avro, Schema Registry)
  rss_producer      poll feeds every 15 min  ─►  news.rss.v1
  reddit_producer   poll subreddits /5 min   ─►  news.reddit.v1
  gdelt_producer    fetch 15-min GKG update  ─►  news.gdelt.v1
                                                      │
                                                      ▼
  Spark Structured Streaming  (jobs/stream_news_to_bronze.py)
    - read all news.* topics
    - deserialize Avro, normalize to common schema
    - watermark + dedupe on (source, source_id)
    - write bronze.news_events_raw (Delta, checkpointed, exactly-once sink)
                                                      │
                                                      ▼
  dbt: silver_news_items  (language filter, URL canonicalization, metro tagging)
                                                      │
                                                      ▼
  Airflow weekly: llm_metro_outlook  ─►  gold_metro_news_outlook
```

### 9.2 Common Event Schema (Avro, `news_item.avsc`)

`source` (rss|reddit|gdelt), `source_id`, `url`, `title`, `body_snippet`, `published_at`, `fetched_at`, `subreddit` (nullable), `feed_name` (nullable), `geo_hint` (nullable: lat/long or place string), `engagement` (nullable: Reddit score/comments).

Schema evolution is backward-compatible only (enforced by Schema Registry compatibility mode). Topic names carry a version suffix.

### 9.3 Metro Tagging

Applied in `silver_news_items`, in priority order:
1. Reddit city subreddit → CBSA via `subreddit_metro_map.yaml`.
2. GDELT location coordinates → county → CBSA.
3. Keyword match of title/snippet against a seed of metro + principal city names (`seeds/metro_aliases.csv`).
4. Otherwise tagged `national`.

### 9.4 LLM Metro Outlook (weekly)

- **Scope:** top ~20 metros by number of scored zips (configurable), plus a `national` summary.
- **Input per metro:** last 7 days of `silver_news_items` for that metro (titles, snippets, Reddit text, capped and ranked by engagement/recency), the metro's GDELT tone trend, and its latest Gold metrics (median rent growth, yield) as context.
- **API usage:**
  - **Message Batches API** — asynchronous, 50% cheaper; fine for a weekly job.
  - **Structured outputs** — response conforms to a JSON schema matching `gold_metro_news_outlook`.
  - **Prompt caching** — a stable system prompt (instructions + output schema) shared across all metros.
  - Model configured in `config/llm.yaml`; default `claude-opus-5-5`, swappable to a cheaper model after comparing output quality.
- **Guardrails:** the prompt requires every claim to cite `cited_item_ids` from the input; a post-check drops any cited ID not present in the input. `stop_reason` is checked (e.g. `refusal`, `max_tokens`) and failed metros are retried or skipped, never written as empty rows.
- **Traceability:** `model_id`, `prompt_version` and the input item IDs are stored with each row. Prompts live in `llm/prompts/` under version control.
- **Evaluation:** a small hand-labeled set (~20 metro-weeks) in `tests/llm_eval/` to compare prompt or model changes before switching.

---

## 10. Data Quality, Lineage & Observability

### 10.1 Data Quality — Two Layers

| Layer | Tool | Examples |
|---|---|---|
| Ingestion (before Bronze) | Great Expectations | Zillow: zip matches `^\d{5}$`, values > 0, month columns parse; FRED: dates monotonic; HMDA: required columns present, row count within ±30% of prior year |
| Modeling (Silver/Gold) | dbt tests (+ `dbt-expectations` package) | `unique`/`not_null` on keys; `gross_rental_yield` between 0 and 30; `price_to_rent_ratio` between 5 and 60; scores between 0 and 100; no future months; national_rank contiguous |
| Freshness | dbt source freshness + Grafana | Bronze tables older than 2× expected cadence → alert |

Failures stop downstream tasks (no silent passes) and send an alert (Slack webhook or email via Airflow callbacks).

### 10.2 Lineage

- **OpenLineage** emitted by Airflow (provider), Spark (OpenLineage listener) and dbt (`dbt-ol` wrapper).
- **Marquez** (Docker) stores and visualizes end-to-end lineage: raw file → Bronze → Silver → Gold → dashboard.

### 10.3 Monitoring

- **Prometheus** scrapes Airflow (StatsD exporter), Kafka (JMX/Kafka exporter: consumer lag), Spark and MinIO metrics.
- **Grafana** dashboards: DAG success/duration, Kafka lag per topic, Bronze freshness, LLM job token usage and cost per run.

---

## 11. Local Platform (Docker Compose)

### 11.1 Services and Profiles

Docker Compose **profiles** keep laptop resource use manageable — run only what the current phase needs.

| Profile | Services | Approx. RAM |
|---|---|---|
| `core` | MinIO, Postgres (Airflow metadata), Airflow (api-server/webserver, scheduler, worker/triggerer) | ~4 GB |
| `spark` | Spark master + 1–2 workers | +2–4 GB |
| `streaming` | Kafka (KRaft, single broker), Schema Registry, Kafka UI, producers, streaming job | +2–3 GB |
| `observability` | Prometheus, Grafana, Marquez (+ its Postgres) | +1–2 GB |

Running everything at once wants **~12–16 GB RAM**. Evidence.dev and dbt run on the host (or in a `tools` container).

### 11.2 Makefile Targets

```
make up            # core profile
make up-all        # all profiles
make down
make test          # pytest (unit) 
make dbt-build     # dbt build against local DuckDB
make lint          # black --check, flake8, sqlfluff
make backfill-fred START=2019-01-01
make dashboard     # evidence dev server
```

### 11.3 CI (GitHub Actions)

On every PR: `black --check`, `flake8`, `sqlfluff lint`, `pytest`, `dbt build` against an ephemeral DuckDB with fixture seeds, and `terraform fmt -check` + `terraform validate` (once `infra/` exists). No cloud credentials needed for CI in the local-first phases.

---

## 12. Cloud Deployment & IAM (Deferred)

Designed now, built in Phase 8 only if/when a public deployment is wanted. Everything is Terraform-managed.

### 12.1 Resources

| Resource | Purpose |
|---|---|
| `google_billing_budget` | **First resource created.** Alerts at 50/90/100% of a small monthly budget (e.g. $25) |
| GCS buckets | `rpof-raw`, `rpof-lake`, `rpof-tfstate` (remote state, versioned) |
| BigQuery datasets | `bronze_ext` (BigLake/external over Delta), `silver`, `gold` |
| Secret Manager | FRED, Census, BLS, Reddit, Anthropic keys |
| Artifact Registry | Container images for producers/jobs |
| Compute Engine (spot, scheduled) | Single VM running the Docker Compose stack when needed |
| Cloud Run / static hosting | Evidence dashboard (static build) |

### 12.2 IAM — Least Privilege

| Service account | Roles (scoped to specific resources) |
|---|---|
| `sa-ingest` | `storage.objectCreator` on `rpof-raw`; `storage.objectAdmin` on `rpof-lake`; `secretmanager.secretAccessor` on ingest secrets only |
| `sa-spark` | `storage.objectViewer` on `rpof-raw`; `storage.objectAdmin` on `rpof-lake` |
| `sa-dbt` | `bigquery.dataEditor` on `silver`/`gold`; `bigquery.dataViewer` on `bronze_ext`; `bigquery.jobUser` (project) |
| `sa-llm` | `bigquery.dataViewer` on `silver`; `bigquery.dataEditor` on `gold`; secret accessor on the Anthropic key only |
| `sa-dashboard` | `bigquery.dataViewer` on `gold` only |
| `sa-github-ci` | Deploy roles only; authenticated via **Workload Identity Federation** — no service-account keys anywhere |

Terraform modules: `infra/modules/{budget,storage,bigquery,iam,secrets,compute}`, one `envs/prod` root. Human access via a group binding, not individual users.

### 12.3 Cloud Cost Guardrails

No Cloud Composer, no managed Kafka, no always-on Dataproc/Databricks clusters, no GKE. VM is spot + start/stop schedule. Budget alert in place before any other resource.

---

## 13. Repo Structure

```
housing-platform/
├── ingestion/                 # Python clients: fred, zillow, census, bls, hud
│   ├── fred_client.py         # ✅ exists
│   ├── storage.py             # S3/MinIO/GCS wrapper (boto3/fsspec)
│   └── bronze_loader.py       # deltalake MERGE helpers
├── streaming/
│   ├── producers/             # rss_producer.py, reddit_producer.py, gdelt_producer.py
│   └── schemas/               # Avro schemas (news_item.avsc)
├── jobs/                      # Spark jobs: hmda_lar.py, gdelt_gkg_filter.py, stream_news_to_bronze.py
├── dags/                      # Airflow DAGs
├── dbt/
│   ├── models/{staging,silver,gold}/
│   ├── seeds/                 # metro_aliases.csv, pinned crosswalk
│   ├── tests/                 # singular tests
│   └── profiles.yml           # local DuckDB target (env-var driven, no secrets)
├── scoring/score.py
├── llm/
│   ├── summarize.py           # builds batch requests, parses results, writes Gold
│   └── prompts/               # versioned prompt templates
├── quality/                   # Great Expectations suites
├── dashboard/                 # Evidence.dev project
├── infra/                     # Terraform (Phase 8)
│   ├── modules/
│   └── envs/prod/
├── docker/                    # Dockerfiles + compose.yaml with profiles
├── observability/             # prometheus.yml, grafana dashboards
├── config/                    # sources.yaml, scoring.yaml, llm.yaml, subreddit_metro_map.yaml
├── tests/                     # pytest unit tests (+ llm_eval/)
├── .github/workflows/ci.yml
├── Makefile
├── requirements*.txt          # split: base / airflow / spark / dev
└── README.md
```

---

## 14. Build Plan — Phases

Each phase ends with something that runs end-to-end. Timelines are rough; phases 0–4 form the **MVP**.

### Phase 0 — Cleanup & Platform Foundation
- [ ] Repo cleanup (see Appendix §18)
- [ ] `docker/compose.yaml` with `core` profile (MinIO, Postgres, Airflow)
- [ ] `ingestion/storage.py` + `ingestion/bronze_loader.py`
- [ ] Commit `dbt/profiles.yml` (DuckDB, env-var driven)
- [ ] GitHub Actions CI: lint + pytest + `dbt build`
- [ ] **Done when:** `make up` starts Airflow + MinIO; CI is green

### Phase 1 — FRED End-to-End *(previously "Step 1.2", in progress)*
- [x] `fred_client.py` + 12 unit tests
- [ ] Align series list with §5.2 (add `UNRATE`)
- [ ] `scripts/backfill_fred.py` (5+ years)
- [ ] Land raw JSON in MinIO → GE suite → `bronze.fred_series_raw` (Delta MERGE)
- [ ] dbt: `stg_fred_series` → `silver_macro_indicators` (+ tests)
- [ ] `dags/ingest_fred.py` triggering `dbt_build` via Dataset
- [ ] **Done when:** weekly DAG runs green and Silver updates automatically

### Phase 2 — Zillow + HUD Crosswalk
- [ ] `ingestion/zillow_client.py` (download + melt) with fixture-CSV tests
- [ ] Bronze `zori_raw`, `zhvi_raw`; full historical backfill
- [ ] HUD ZIP→CBSA and TRACT→ZIP crosswalks
- [ ] dbt `silver_rent_index`, `silver_home_values` (+ range tests)
- [ ] `dags/ingest_zillow.py`
- [ ] **Done when:** you can query rent + home value history for any covered zip

### Phase 3 — Census + BLS + Gold + Scoring
- [ ] `census_client.py`, `bls_client.py` → Bronze → Silver
- [ ] `gold_zip_investment_metrics`
- [ ] `scoring/score.py` + `config/scoring.yaml` + `tests/test_score.py`
- [ ] Gold dbt tests; `data_quality_report` DAG with alerts
- [ ] **Done when:** `SELECT * FROM gold.zip_opportunity_score ORDER BY national_rank LIMIT 20` is meaningful

### Phase 4 — Dashboard (MVP complete)
- [ ] Evidence.dev project on local DuckDB
- [ ] Pages: National Overview, Zip Deep-Dive, Metro Comparison, Screener
- [ ] **Done when:** you can find top zips and see full score decomposition — **MVP**

### Phase 5 — Spark Batch: HMDA (+ GDELT history)
- [ ] `spark` compose profile with `delta-spark`
- [ ] `jobs/hmda_lar.py`; backfill 2018–latest
- [ ] `silver_spark.zip_lending_activity` → join into Gold
- [ ] `jobs/gdelt_gkg_filter.py` → `silver_spark.gdelt_metro_tone`
- [ ] Airflow `SparkSubmitOperator` DAGs
- [ ] **Done when:** investor purchase share appears on the Zip Deep-Dive page

### Phase 6 — Streaming News + LLM Outlook
- [ ] `streaming` compose profile: Kafka (KRaft), Schema Registry, Kafka UI
- [ ] Producers: RSS → Reddit → GDELT live
- [ ] `jobs/stream_news_to_bronze.py` (Structured Streaming, checkpointed)
- [ ] dbt `silver_news_items` with metro tagging
- [ ] `llm/summarize.py` + `llm_metro_outlook` DAG (Batch API, structured outputs)
- [ ] Dashboard page 5: **Market Pulse** (outlook per metro, themes, cited sources)
- [ ] **Done when:** weekly outlooks populate automatically with valid citations

### Phase 7 — Lineage & Observability
- [ ] `observability` profile: Marquez, Prometheus, Grafana
- [ ] OpenLineage on Airflow, Spark, dbt
- [ ] Grafana: DAG health, Kafka lag, freshness, LLM cost per run
- [ ] **Done when:** one lineage graph shows raw file → dashboard

### Phase 8 — Cloud Deployment (optional)
- [ ] Terraform: budget alert first, then buckets, BigQuery, secrets, IAM (§12.2), WIF for GitHub Actions
- [ ] dbt `bigquery` target; dashboard deployed as static site
- [ ] **Done when:** public URL live; monthly cloud bill under budget

### Phase 9 — Stretch
Redfin, HUD FMR, score v2 (§7.5), tier-change alerts, dbt snapshots for score history, Databricks Free Edition portability demo, README + architecture diagram + demo video.

---

## 15. Cost Model

### 15.1 Local-First (Phases 0–7)

| Item | Monthly cost |
|---|---|
| All data sources | $0 (free keys for FRED, Census, BLS, Reddit) |
| Docker stack (Airflow, Spark, Kafka, MinIO, observability) | $0 — runs on your machine |
| GitHub + GitHub Actions (public repo) | $0 |
| **LLM outlook (Claude Batch API)** | **~$1–6** — see below |
| **Total** | **~$1–6 / month** |

**LLM estimate** (assumes ~30k input tokens + ~1k output per metro, Batch API 50% discount, list prices as of Nov 2026 — verify on the pricing page):

| Cadence / scope | Opus 5.5 (default) | Sonnet 5.5 | Haiku 5.5 |
|---|---|---|---|
| Weekly, 20 metros | ~$6 | ~$3 | <$1 |
| Daily, 50 metros | ~$100 | ~$50 | ~$3 |

Weekly is the planned cadence. Prompt caching on the shared system prompt reduces cost further.

### 15.2 Cloud (Phase 8, optional)

| Item | Monthly cost |
|---|---|
| GCS (few GB) | <$1 |
| BigQuery | $0 within free tier (1 TB query / 10 GB storage) |
| Secret Manager, Artifact Registry | <$1 |
| Dashboard (static hosting) | $0 |
| Spot VM for Airflow/Kafka, scheduled | ~$10–15 (≈$25–50 if always on) |
| **Total (+ LLM)** | **~$12–30** |

New GCP accounts' free-trial credits would likely cover the first months. **Avoid:** Cloud Composer, managed Kafka, always-on Dataproc/Databricks, GKE, Snowflake after trial.

---

## 16. Tech Stack & Decision Log

### 16.1 Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow (Docker; data-aware scheduling) |
| Batch processing | Python (small sources), Apache Spark + delta-spark (large sources) |
| Streaming | Apache Kafka (KRaft) + Schema Registry (Avro), Spark Structured Streaming |
| Object storage | MinIO (local) → GCS (cloud) |
| Table format | Delta Lake |
| Transformation | dbt (dbt-duckdb → dbt-bigquery) |
| Warehouse / query | DuckDB (local) → BigQuery (cloud) |
| Scoring | Python + pandas |
| AI summarization | Claude via Message Batches API, structured outputs, prompt caching |
| Data quality | Great Expectations, dbt tests, dbt-expectations, source freshness |
| Lineage | OpenLineage + Marquez |
| Monitoring | Prometheus + Grafana |
| Dashboard | Evidence.dev |
| IaC / IAM | Terraform (GCP), Workload Identity Federation |
| CI/CD | GitHub Actions |
| Dev tooling | Docker Compose, pytest, black, flake8, sqlfluff, pre-commit |

### 16.2 Decision Log

| # | Decision | Alternatives | Rationale |
|---|---|---|---|
| D1 | Local-first; cloud deferred | Cloud from day one | Near-zero cost while building; same code deploys later |
| D2 | Delta Lake | Apache Iceberg | Already in deps; first-class in `deltalake` (Python), DuckDB `delta_scan`, Spark; keeps Databricks option open. Iceberg would be equally valid. |
| D3 | BigQuery for cloud | Snowflake | Free tier covers this scale; native GCP IAM; no trial cliff |
| D4 | No Databricks (for now) | Databricks paid / Free Edition | Avoids cost and splitting the platform; Spark jobs kept portable so it can be added later |
| D5 | Self-hosted Kafka (KRaft) | Confluent Cloud, Google Managed Kafka | Managed Kafka has fixed costs that don't fit a hobby budget |
| D6 | Spark only for HMDA/GDELT/streaming | Spark for everything | Small sources are faster and simpler in Python/DuckDB; shows judgment about tool fit |
| D7 | BLS CES (monthly) over QCEW | QCEW (quarterly) | Timeliness |
| D8 | RSS/Reddit API/GDELT over raw article scraping | Scrape full articles | Respects site terms and paywalls; GDELT provides scale legitimately |
| D9 | LLM weekly via Batch API | Daily / real-time | Rental markets move slowly; ~95% cheaper than daily, 50% batch discount |
| D10 | Scoring in Python, not SQL | dbt-only | Cross-zip percentile ranking + config-driven weights are clearer in pandas |

---

## 17. Risks & Mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Zillow changes CSV format/URL | Medium | Raw files kept immutable; GE schema checks fail loudly; only Silver breaks |
| ZORI covers far fewer zips than ZHVI | High | Expected. Score only where both exist; show coverage on dashboard |
| ZCTA ≠ zip; tract→zip allocation is approximate | High | Documented approximation; use HUD `res_ratio` weights; label metrics as estimates |
| HMDA/BLS metrics are metro- or year-level | Medium | Carry vintage columns; show "as of" on dashboard |
| Reddit API terms/limits change | Medium | Reddit is one of three news sources; pipeline tolerates a missing producer |
| LLM hallucination in outlooks | Medium | Citation-required prompt + post-check against input IDs; store inputs for audit; show sources on dashboard |
| LLM cost creep | Low | Weekly cadence, Batch API, per-run token logging in Grafana, hard cap on items per metro |
| Laptop resources (Kafka + Spark + Airflow) | Medium | Compose profiles; run only what the current phase needs |
| Scope creep / never finishing | **High** | MVP = Phases 0–4. Each phase ends runnable. Streaming and cloud come after a working product. |
| Accidental cloud spend (Phase 8) | Low | Budget alert is the first Terraform resource; spot + scheduled VM; no fixed-fee managed services |
| Secrets leaked to git | Low | `.env` gitignored; pre-commit `detect-secrets`; Secret Manager + WIF in cloud |

---

## 18. Appendix: Repo Cleanup Backlog

Found during review on 2026-11-11:

- [ ] `data/housing.duckdb` and `logs/dbt.log` are committed despite `.gitignore` → `git rm --cached`.
- [ ] `requirements.txt`: remove `mlflow`, `xgboost`, `scikit-learn`; split into base / airflow / spark / dev; reconsider Airflow version (currently pinned 2.9.1) before building DAGs.
- [ ] `.env.example`: remove RapidAPI and Walk Score keys; add `BLS_API_KEY`, `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, `REDDIT_USER_AGENT`, `ANTHROPIC_API_KEY`; rename bucket vars to `rpof-raw` / `rpof-lake`.
- [ ] `ingestion/fred_client.py`: add `UNRATE`; replace `print` with `logging`; add request timeout and retry/backoff.
- [ ] `dbt/`: delete `models/example/`; add `profiles.yml`; update `dbt_project.yml` model configs for `staging/silver/gold`.
- [ ] README: replace one-line title with overview, architecture diagram, quickstart (`make up`).
- [ ] Add `sqlfluff` and `detect-secrets` to pre-commit.

---

*Document Version: 3.0*
*Revised from: v2.0 (Rental Property Opportunity Finder) and v1.0 (Housing Market Intelligence Platform).*
*Key change: Local-first platform adding Spark (HMDA, GDELT), Kafka streaming news ingestion, a weekly LLM market-outlook layer, data quality/lineage/observability, CI from day one, and a deferred Terraform-managed GCP deployment with least-privilege IAM. Databricks and Snowflake omitted for cost.*
