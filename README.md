# Rental Property Opportunity Finder

A local-first data platform that ranks US zip codes for rental property investment using only free, open data. Batch sources (Zillow, FRED, Census, BLS, HUD, HMDA) and streaming sources (news, Reddit, GDELT) flow through a Bronze → Silver → Gold lakehouse, producing an explainable opportunity score and a weekly AI-summarized market outlook.

See the full design: [`rental_opportunity_finder_design_doc.md`](rental_opportunity_finder_design_doc.md).

## Status

| Phase | Scope | Status |
|---|---|---|
| 0 | Repo cleanup, Docker stack, storage + Bronze loader, CI | ✅ |
| 1 | FRED end-to-end (Bronze → Silver, Airflow DAG) | ✅ |
| 2 | Zillow rents + home values, HUD zip↔metro/tract crosswalks | In progress: Zillow live, HUD awaiting real-data check |
| 3–4 | Census/BLS, scoring, dashboard (MVP) | Planned |
| 5–9 | Spark (HMDA/GDELT), Kafka streaming + LLM outlook, lineage/monitoring, cloud | Planned |

## Stack (current)

Airflow 3 · SeaweedFS (S3-compatible object storage) · Delta Lake (`deltalake`) · Great Expectations · dbt + DuckDB · Docker Compose · GitHub Actions

## Quickstart

Requirements: Docker with ~4 GB RAM free, Python 3.11.

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
