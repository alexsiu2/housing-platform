# Rental Property Opportunity Finder

A local-first data platform that ranks US zip codes for rental property investment using only free, open data. Batch sources (Zillow, FRED, Census, BLS, HUD, HMDA) and streaming sources (news, Reddit, GDELT) flow through a Bronze → Silver → Gold lakehouse, producing an explainable opportunity score and a weekly AI-summarized market outlook.

See the full design: [`rental_opportunity_finder_design_doc.md`](rental_opportunity_finder_design_doc.md).

## Status

| Phase | Scope | Status |
|---|---|---|
| 0 | Repo cleanup, Docker stack, storage + Bronze loader, CI | ✅ |
| 1 | FRED end-to-end (Bronze → Silver, Airflow DAG) | ✅ |
| 2 | Zillow rents + home values, HUD zip↔metro/tract crosswalks | ✅ |
| 3 | Census ACS + BLS jobs, Gold metrics, opportunity score, data-quality alerts | ✅ |
| 4 | Dashboard: overview, zip deep-dive, metros, screener (**MVP**) | ✅ |
| 5 | Spark batch: HMDA mortgage records → investor purchase share by zip; GDELT housing news via BigQuery | ✅ |
| 6 | Streaming news: RSS + live GDELT → Kafka → Spark → Bronze → metro-tagged `silver_news_items`; weekly Claude metro outlook + Market Pulse page | ✅ |
| 7–8 | Lineage/monitoring, cloud | Planned |

## Stack (current)

Airflow 3 · SeaweedFS (S3-compatible object storage) · Delta Lake (`deltalake`) · Great Expectations · dbt + DuckDB · Apache Spark 4.2 + Delta · Evidence.dev · Docker Compose · GitHub Actions

## Quickstart

Requirements: Docker with ~4 GB RAM free (8 GB recommended once Zillow is loaded: the in-Airflow `dbt build` peaks around 2.3 GB on the full ZHVI table), Python 3.11.

```bash
cp .env.example .env          # then add your FRED_API_KEY
make setup                    # Python dev deps + pre-commit hooks
make up                       # SeaweedFS + Postgres + Airflow
```

- Airflow UI: http://localhost:8080 (user/password `airflow` / `airflow` by default). If 8080 is taken, `make up` stops and names what holds it (or check yourself with `docker ps --filter publish=8080` / `lsof -i :8080`); stop that, or set `AIRFLOW_PORT=8081` in `.env`.
- Object storage (S3 API): http://localhost:8333 — buckets `rpof-raw` and `rpof-lake` are created on startup

Run `make smoke-test` (or trigger **`platform_smoke_test`** in the UI) to confirm Airflow can write to object storage and to a Delta table.

## FRED pipeline (Phase 1)

```
ingest_fred (weekly, Mon 06:00 UTC)               dbt_build (on Bronze asset update)
  extract      FRED API -> s3://rpof-raw/fred/...      dbt build --select source:bronze+
  load_bronze  Great Expectations -> Delta MERGE  ──►    stg_fred__observations
               bronze.fred_series_raw (asset)            silver_macro_indicators
```

```bash
make backfill-fred START=2019-01-01   # history -> raw -> GE -> Bronze (host, needs FRED_API_KEY)
make run-dag DAG=dbt_build            # rebuild Silver in Airflow (or: make dbt-build on the host)
make run-dag DAG=ingest_fred          # one weekly run end-to-end; triggers dbt_build
```

Scheduled runs re-fetch the last 2 years because FRED revises recent values; the MERGE on `(series_id, date)` keeps re-runs idempotent.

## Zillow + HUD (Phase 2)

| DAG | Schedule | What it loads |
|---|---|---|
| `ingest_zillow` | Monthly, 20th | ZORI rents + ZHVI home values by zip → `bronze.zori_raw`, `bronze.zhvi_raw` (re-merges the last 24 months) |
| `ingest_hud_crosswalk` | Quarterly | HUD zip→CBSA and tract→zip crosswalks → `bronze.hud_crosswalk_raw` (needs `HUD_API_TOKEN`) |

Silver: `silver_rent_index`, `silver_home_values` (zip × month, 1-year growth, primary CBSA), `silver_zip_cbsa_xwalk`, `silver_tract_zip_xwalk`.

```bash
make backfill-zillow                  # full ZORI (2015+) + ZHVI (2000+) history
make load-hud                         # latest HUD crosswalk vintage
```

## Scoring (Phase 3)

`gold.zip_opportunity_score`: one row per zip for the latest month, a 0–100 composite of five percentile sub-scores (design doc §7). Weights live in [`config/scoring.yaml`](config/scoring.yaml), and every run records `weights_version`.

| Sub-score | Metric | Better | Weight |
|---|---|---|---|
| rental yield | 12 × rent ÷ home value | higher | 30% |
| rent growth | rent vs 12 months earlier | higher | 25% |
| affordability | 12 × rent ÷ median household income | lower | 20% |
| entry price | home value ÷ median household income | lower | 15% |
| demand growth | metro jobs vs 12 months earlier | higher | 10% |

A zip is scored only with yield, rent growth and affordability, and with population ≥ 1,000. If metro job data is missing (outside a CES metro, or > 3 months stale), demand growth scores a neutral 50 and `partial_score` is true. Current weights: `v1.1` (changelog in design doc §7.6). Compare any two configs with `python scripts/compare_scores.py`.

```bash
make load-census                      # ACS 5-year vintages 2019+ by ZCTA (needs CENSUS_API_KEY)
make backfill-bls                     # metro employment 2014+ (needs BLS_API_KEY + HUD in Bronze)
make dbt-build && make score          # Gold metrics, then scores
```

In Airflow, `compute_scores` runs after every green `dbt_build`. `data_quality_report` checks Bronze freshness daily, and every DAG alerts on failure (set `ALERT_WEBHOOK_URL` for Slack).

## Dashboard (Phase 4)

```bash
make dashboard         # http://localhost:3000 (needs Node 18–22; `brew install node@22`)
make dashboard-build   # static site in dashboard/build
```

| Page | What it answers |
|---|---|
| National Overview | Where are the best opportunities? Top 20 with every sub-score, strong zips by state, score distribution, macro backdrop, how the score works |
| Zip Deep-Dive | Why does this zip score what it does? Points per sub-score (they add up to the score), raw metrics, ranks, rent and value history |
| Metro Comparison | Which metros look best, and which zips inside one? |
| Screener | Filter all scored zips by state, tier, score, yield, price and job-data coverage; download CSV |
| Market Pulse | What is this week's housing news saying about each big metro? Claude's weekly outlook, themes, risks and the cited sources |

The dashboard reads `data/dashboard.duckdb`, a read-only extract Airflow refreshes after every scoring run (see [`dashboard/README.md`](dashboard/README.md) for why it is separate from the warehouse).

## Spark: HMDA mortgage data (Phase 5)

Every US home-mortgage application (HMDA, 2018 onward, ~10–25M rows a year) goes through Spark to estimate, per zip, how many home purchases were by **investors**:

```
spark_hmda (annual, mid-July; or manual with {"years": [...]})
  download  FFIEC snapshot zip -> raw zone        stage  zip -> .csv.gz (Spark can't read zip)
  hmda_lar  SparkSubmitOperator -> jobs/hmda_lar.py on the `spark` profile
            originated 1-4 unit home purchases -> bronze.hmda_lar_raw
            tract counts -> HUD tract->zip allocation -> silver_spark.zip_lending_activity
```

```bash
make up-spark                          # core + Spark master/worker (start only when needed)
make backfill-hmda                     # 2018 through last year, in Airflow; or YEARS="2024 2025"
```

Spark is sized to 2 cores / 2.5 GB because the Docker VM is shared; stop it when the load is done (`docker compose -f docker/compose.yaml --profile spark stop spark-master spark-worker`). Tract boundaries changed in 2020, and HMDA and HUD switched in different years, so the job picks each year's crosswalk by **measured** match rate and reports it. Investor share is shown on the Zip Deep-Dive and Screener as context; it is not a score input (design §7.5).

## Natural hazard (FEMA National Risk Index)

Expected yearly **building** damage from natural hazards, as a share of building value, per zip (FEMA NRI tracts allocated to zips), with the top peril and a national percentile. Shown on the Zip Deep-Dive (with a warning for the riskiest 10%) and as a Screener filter. It is **context, not a score input**: high-scoring zips turned out not to skew toward hazard (design doc D14). `ingest_fema_nri` runs yearly (FEMA releases in December); `make load-nri` loads it by hand. Data comes from FEMA's own ArcGIS feature service, because fema.gov's zip downloads refuse scripted requests.

Asset-triggered `dbt_build` runs rebuild only what is downstream of the data that changed (`dags/dbt_selection.py`). A FEMA update rebuilds 27 of 110 nodes in about 11 seconds instead of reloading every Bronze source.

## Housing news (GDELT via BigQuery)

GDELT's news graph is 22 TB, so the filter runs **in BigQuery** (a free public dataset) and only US housing articles come back, about 5,000 a week. They land in `bronze.gdelt_housing_articles_raw`, and dbt turns them into weekly metro volume and tone (`silver_gdelt_metro_tone`) and national tone, shown on the Metro Comparison page. `ingest_gdelt_news` runs every Monday for the last full week (~3 GB scanned); `make backfill-gdelt START=2024-01-01` loads history (~345 GB, inside the free 1 TB/month).

**Setup (one time):** a Google Cloud project with the BigQuery sandbox (no billing, so it can't charge), a service account with **BigQuery Job User** only, and keyless login:

```bash
gcloud auth application-default login --impersonate-service-account=<reader>@<project>.iam.gserviceaccount.com
```

Then set `GCP_PROJECT=<project>` in `.env`. Docker mounts only that credentials file, read-only. Every query is dry-run against a budget before it runs (design doc D15).

**Known gaps:** GDELT itself has almost no data for the weeks of 2025-06-16 and 2025-06-23, so those weeks are missing rather than zero. Most metros see fewer than 10 housing articles a week (`is_low_sample`), and Washington, DC picks up national policy news (`is_national_policy_hub`); the dashboard flags both.

## Streaming news (Kafka)

```bash
make up-streaming   # core stack + Kafka, Schema Registry, Kafka UI, news producers
```

Two producers poll every 15 minutes and publish Avro events (`streaming/schemas/news_item.avsc`) through Schema Registry: `rss-producer` reads the housing feeds in `config/rss_feeds.yaml` (headlines and summaries only; robots.txt honoured), and `gdelt-producer` reads GDELT's live 15-minute file, keeping the same US housing articles as the weekly BigQuery pull. The Airflow DAG `stream_news_to_bronze` runs a Spark Structured Streaming job with `trigger(availableNow)` every 15 minutes: it drains new events into `bronze.news_events_raw` (deduped on source + id) and exits, so no Spark process idles (design doc D16). dbt builds `silver_news_items` daily, tagging each item with metros from GDELT locations or city names (`dbt/seeds/metro_aliases.csv`, generated by `scripts/make_metro_aliases.py`). Kafka UI is at http://localhost:8091.

Turn on `stream_news_to_bronze` in the Airflow UI after the first `make up-streaming`. If Kafka isn't running, its runs are skipped, not failed. Reddit is planned once API access is approved.

## Weekly metro outlook (Claude)

Every Monday the Airflow DAG `llm_metro_outlook` summarizes the previous week's news (`silver_news_items`) for the 20 metros with the most scored zips, plus the US as a whole, using the Claude Message Batches API (design doc §9.4, D17). Each outlook is bullish / neutral / bearish with a confidence, a 3–5 sentence summary in which every sentence cites its news items, themes and risks. It lands in `gold.metro_news_outlook` (+ `gold.metro_news_outlook_citations`) and on the dashboard's Market Pulse page.

- Needs `ANTHROPIC_API_KEY` in `.env`. Model and limits are in `config/llm.yaml` (default `claude-haiku-5-5`, about $0.20 a month); the prompt is `llm/prompts/metro_outlook_v1.md`.
- Guardrails: the answer must end normally and match a JSON schema; citations to items that weren't in the input are dropped, and an outlook with too few cited sentences is rejected. Rejected or failed requests are retried once directly; refusals are skipped. Metros with fewer than 3 items that week get no outlook, never an empty one.
- Every request and result is landed in the raw bucket under `llm/metro_outlook/week_start=…/` for audit.
- Turn the DAG on in the Airflow UI. To redo or backfill a week, trigger it with `week_start` (a Monday); that week's rows are replaced. Locally:

```bash
python -m llm.summarize --week-start 2026-11-16 --dry-run   # inputs and token sizes, no API calls
python -m llm.summarize --week-start 2026-11-16             # batch; waits for it (usually minutes)
```

News only flows while the stack is running: if the laptop sleeps, the live GDELT files from that time are not fetched (the weekly BigQuery pull still covers GDELT tone).

## One warehouse writer at a time

`data/rpof.duckdb` is written by Airflow (dbt, scoring, the dashboard export) and by `make dbt-build` / `make score` on the host. DuckDB's own file lock does **not** work across Docker Desktop's file sharing, so two writers once corrupted the file. Every writer now takes `data/.warehouse.lock` first (`ingestion/warehouse_lock.py`), and a second writer waits. If a crash leaves the lock behind, it is broken automatically after 3 hours, or you can remove the `data/.warehouse.lock` folder by hand. The warehouse is always rebuildable from the lake: `make dbt-build && make score`.

## Common commands

```bash
make test        # unit tests
make smoke-test  # platform_smoke_test against the running stack
make lint        # black --check + flake8
make dbt-build   # dbt models + tests against data/rpof.duckdb
make logs        # tail the stack logs
make down        # stop the stack (data volumes are kept)
make help        # list all targets
```

## Layout

```
ingestion/   API/CSV clients, object storage helpers, Delta Bronze loader
dags/        Airflow DAGs
dbt/         staging / silver / gold models
docker/      compose.yaml (profiles per phase), Airflow image, SeaweedFS config
tests/       pytest unit tests
```
