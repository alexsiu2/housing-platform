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
| 6–9 | Kafka streaming + LLM outlook, lineage/monitoring, cloud | Planned |

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
