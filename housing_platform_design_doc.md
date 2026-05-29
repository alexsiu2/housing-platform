# Housing Market Intelligence Platform
## Technical Design Document v1.0

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Goals & Success Metrics](#2-goals--success-metrics)
3. [Architecture Overview](#3-architecture-overview)
4. [Data Sources](#4-data-sources)
5. [Data Model & Schema Design](#5-data-model--schema-design)
6. [Pipeline Design](#6-pipeline-design)
7. [ML Layer Design](#7-ml-layer-design)
8. [Dashboard & Serving Layer](#8-dashboard--serving-layer)
9. [Infrastructure & DevOps](#9-infrastructure--devops)
10. [Project Phases — Step-by-Step Build Plan](#10-project-phases--step-by-step-build-plan)
11. [Tech Stack Summary](#11-tech-stack-summary)
12. [Risks & Mitigations](#12-risks--mitigations)

---

## 1. Project Overview

**Project Name:** Housing Market Intelligence Platform (HMIP)

**One-Line Summary:** A production-grade data engineering system that ingests multi-source housing data (listings, macroeconomics, demographics, neighborhood features), transforms it through a medallion lakehouse architecture, and serves market insights and ML-powered predictions through a live dashboard.

**Motivation:** Housing is the most significant financial decision most people make, yet the data ecosystem around it is fragmented — listing portals don't integrate with mortgage rate trends, schools, or census demographics. This platform unifies those signals into a single analytical layer.

**What makes this portfolio-worthy:**
- End-to-end pipeline with real orchestration, not a notebook
- Multi-source data fusion requiring real schema reconciliation work
- Production tooling: dbt tests, MLflow, CI/CD
- Live deployed dashboard with a real URL
- Business-framed outputs (affordability index, market momentum scores)

---

## 2. Goals & Success Metrics

### Primary Goals
| Goal | Definition of Done |
|------|-------------------|
| Ingest housing listings data | Daily refresh of active listings for 5+ metros |
| Integrate macroeconomic signals | Mortgage rates, CPI, housing starts from FRED (weekly refresh) |
| Integrate demographic context | Census zip-level income, population, household size |
| Build a medallion lakehouse | Bronze/Silver/Gold layers with dbt managing transformations |
| ML price prediction model | RMSE < 15% of median listing price for test markets |
| Market momentum scoring | Per-zip "heating/cooling/stable" label, updated weekly |
| Affordability index | Monthly price-to-income and mortgage-burden ratio by metro |
| Live dashboard | Deployed Superset/Evidence instance with public URL |
| Data quality layer | dbt tests + Great Expectations on all Gold tables |

### Stretch Goals
- Streaming ingestion for new listings via Pub/Sub or Kafka
- MLflow experiment tracking and model registry
- Neighborhood comparison feature in the dashboard
- Price alert system (email/webhook when a zip's momentum changes)

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                               │
│                                                                       │
│  [Zillow/RapidAPI]  [FRED API]  [Census API]  [Walk Score API]       │
│         └──────────────┴──────────────┴──────────────┘              │
│                              │                                        │
│                   Python ingest scripts                               │
│                   (called by Airflow DAGs)                           │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        STORAGE LAYER                                  │
│                                                                       │
│   GCS Bucket: housing-platform-raw/                                  │
│     ├── listings/YYYY/MM/DD/*.json                                   │
│     ├── fred/YYYY/MM/DD/*.json                                       │
│     ├── census/YYYY/*.csv                                            │
│     └── walkscore/YYYY/MM/DD/*.json                                  │
│                                                                       │
│   BRONZE Delta Tables (raw, append-only)                             │
│     ├── bronze.listings_raw                                          │
│     ├── bronze.fred_series_raw                                       │
│     ├── bronze.census_acs_raw                                        │
│     └── bronze.walkscore_raw                                         │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    TRANSFORMATION LAYER (dbt)                         │
│                                                                       │
│   SILVER (cleaned, typed, deduplicated, joined)                      │
│     ├── silver.listings          (normalized listing records)         │
│     ├── silver.mortgage_rates    (FRED series, forward-filled)       │
│     ├── silver.zip_demographics  (Census ACS, zip-level)             │
│     └── silver.zip_scores        (walkability, transit, school)      │
│                                                                       │
│   GOLD (business-ready metrics and ML features)                      │
│     ├── gold.zip_market_metrics  (DOM, price cuts, list-to-sale)     │
│     ├── gold.metro_affordability (price/income, mortgage burden)     │
│     ├── gold.listing_features    (ML feature table)                  │
│     └── gold.market_momentum     (heating/cooling labels per zip)    │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                    ┌────────┴────────┐
                    ▼                 ▼
┌─────────────────────────┐  ┌──────────────────────────────────────┐
│      ML LAYER           │  │         SERVING LAYER                 │
│                         │  │                                        │
│  Snowflake / DuckDB     │  │  Snowflake / DuckDB                  │
│  Gold tables → features │  │  ← queried by Superset/Evidence      │
│                         │  │                                        │
│  Models:                │  │  Dashboards:                          │
│  - Price Estimator      │  │  - National Affordability Heatmap     │
│    (XGBoost / LightGBM) │  │  - Market Temperature by Zip          │
│  - Momentum Classifier  │  │  - Metro Time-Series Explorer         │
│  - Affordability Index  │  │  - Price Prediction Tool              │
│                         │  │  - Neighborhood Comparison            │
│  MLflow tracking        │  │                                        │
└─────────────────────────┘  └──────────────────────────────────────┘
                    │                        │
                    └──────────┬─────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION (Airflow)                            │
│                                                                       │
│   DAGs:                                                               │
│   - ingest_listings_daily        (every morning, 6am)               │
│   - ingest_fred_weekly           (Mondays)                           │
│   - ingest_census_monthly        (1st of month)                      │
│   - run_dbt_transformations      (after each ingest DAG)             │
│   - retrain_price_model_weekly   (Sundays)                           │
│   - data_quality_checks          (after dbt runs)                    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 4. Data Sources

### 4.1 Zillow via RapidAPI (Primary Listings Source)
- **What:** Active listings, price history, days on market, beds/baths/sqft, list-to-sale ratio
- **How:** RapidAPI Zillow endpoint (free tier: ~500 req/month; paid: ~$10/month)
- **Endpoint examples:**
  - `GET /search` — listings by zip or metro
  - `GET /property` — detail for a single property ZPID
- **Ingest pattern:** Paginated JSON → GCS raw → Bronze Delta table
- **Cadence:** Daily

### 4.2 FRED API (Macroeconomic Signals)
- **What:** 30-year mortgage rate, CPI-Housing component, housing starts by metro, median sale price indices
- **How:** St. Louis Fed public API — completely free, just need an API key
- **Series to pull:**
  - `MORTGAGE30US` — 30-year fixed rate
  - `CUUR0000SAH` — CPI shelter component
  - `HOUST` — housing starts national
  - `MSPUS` — median sale price US
  - `CSUSHPISA` — Case-Shiller Home Price Index
- **Cadence:** Weekly (FRED updates most series weekly/monthly)

### 4.3 US Census Bureau ACS API
- **What:** Zip-level median household income, population, household size, owner vs. renter ratio
- **How:** Census API — free, API key optional for low volume
- **Tables to pull:**
  - `B19013` — Median household income by zip
  - `B01003` — Total population
  - `B25003` — Tenure (owner vs. renter)
  - `B25064` — Median gross rent
- **Cadence:** Annual (ACS 5-year estimates, released December each year)

### 4.4 Walk Score API
- **What:** Walkability, transit score, bike score per address/zip
- **How:** Walk Score API — free tier (5,000 req/day)
- **Cadence:** One-time load per zip, refresh quarterly

### 4.5 NCES / GreatSchools API
- **What:** School ratings by district/zip
- **How:** GreatSchools API (free dev key) or NCES public CSV downloads
- **Cadence:** Annual

### 4.6 HUD USPS Crosswalk Files
- **What:** Zip-to-CBSA (metro area) mappings — critical for joining sources at the right geo level
- **How:** HUD website public download (quarterly)
- **Cadence:** Quarterly

---

## 5. Data Model & Schema Design

### 5.1 Bronze Layer (Raw Ingestion — No Transformations)

```sql
-- bronze.listings_raw
CREATE TABLE bronze.listings_raw (
    ingestion_id        STRING,          -- UUID generated at ingest time
    source              STRING,          -- 'zillow', 'redfin', etc.
    ingested_at         TIMESTAMP,       -- pipeline run timestamp
    listing_date        DATE,            -- date of the listing snapshot
    raw_payload         STRING           -- full JSON blob (schema-on-read)
);

-- bronze.fred_series_raw
CREATE TABLE bronze.fred_series_raw (
    series_id           STRING,          -- 'MORTGAGE30US', etc.
    observation_date    DATE,
    value               FLOAT,
    ingested_at         TIMESTAMP
);

-- bronze.census_acs_raw
CREATE TABLE bronze.census_acs_raw (
    zip_code            STRING,
    table_name          STRING,          -- 'B19013', etc.
    variable            STRING,          -- e.g. 'B19013_001E'
    value               FLOAT,
    vintage_year        INT,
    ingested_at         TIMESTAMP
);
```

### 5.2 Silver Layer (Cleaned, Typed, Deduplicated)

```sql
-- silver.listings
CREATE TABLE silver.listings (
    listing_id          STRING PRIMARY KEY,
    zpid                STRING,           -- Zillow property ID
    source              STRING,
    listing_date        DATE,
    zip_code            STRING,
    metro_cbsa          STRING,           -- joined from HUD crosswalk
    city                STRING,
    state               STRING,
    latitude            FLOAT,
    longitude           FLOAT,
    property_type       STRING,           -- 'single_family', 'condo', etc.
    bedrooms            INT,
    bathrooms           FLOAT,
    sqft                INT,
    lot_size_sqft       INT,
    year_built          INT,
    list_price          FLOAT,
    price_per_sqft      FLOAT,
    days_on_market      INT,
    price_cuts          INT,              -- number of price reductions
    last_price_cut_pct  FLOAT,            -- % of last reduction
    listing_status      STRING,           -- 'active', 'pending', 'sold'
    sale_price          FLOAT,            -- null if not sold
    list_to_sale_ratio  FLOAT,            -- sale_price / list_price
    updated_at          TIMESTAMP
);

-- silver.zip_demographics
CREATE TABLE silver.zip_demographics (
    zip_code                STRING PRIMARY KEY,
    cbsa_code               STRING,
    median_household_income FLOAT,
    total_population        INT,
    owner_occupied_pct      FLOAT,
    renter_occupied_pct     FLOAT,
    median_gross_rent       FLOAT,
    vintage_year            INT
);

-- silver.mortgage_rates
CREATE TABLE silver.mortgage_rates (
    observation_date    DATE PRIMARY KEY,
    rate_30yr_fixed     FLOAT,
    cpi_shelter         FLOAT,
    housing_starts      INT,
    case_shiller_index  FLOAT,
    median_sale_price   FLOAT
);
```

### 5.3 Gold Layer (Business Metrics & ML Features)

```sql
-- gold.zip_market_metrics (updated weekly)
CREATE TABLE gold.zip_market_metrics (
    zip_code                STRING,
    snapshot_week           DATE,
    active_listings         INT,
    median_list_price       FLOAT,
    median_price_per_sqft   FLOAT,
    median_days_on_market   FLOAT,
    pct_listings_with_cut   FLOAT,    -- % that had a price reduction
    avg_list_to_sale_ratio  FLOAT,
    new_listings_7d         INT,
    sold_listings_7d        INT
);

-- gold.metro_affordability (updated monthly)
CREATE TABLE gold.metro_affordability (
    cbsa_code                   STRING,
    metro_name                  STRING,
    snapshot_month              DATE,
    median_list_price           FLOAT,
    median_household_income     FLOAT,
    price_to_income_ratio       FLOAT,   -- median_price / median_income
    monthly_mortgage_payment    FLOAT,   -- at current 30yr rate, 20% down
    mortgage_burden_pct         FLOAT,   -- mortgage_payment / (income/12)
    affordability_tier          STRING,  -- 'affordable', 'stretched', 'unaffordable'
    mortgage_rate_used          FLOAT
);

-- gold.listing_features (ML feature table)
CREATE TABLE gold.listing_features (
    listing_id              STRING PRIMARY KEY,
    -- property features
    bedrooms                INT,
    bathrooms               FLOAT,
    sqft                    INT,
    lot_size_sqft           INT,
    year_built              INT,
    property_type           STRING,
    -- location features
    zip_code                STRING,
    metro_cbsa              STRING,
    latitude                FLOAT,
    longitude               FLOAT,
    walk_score              INT,
    transit_score           INT,
    school_rating           FLOAT,
    -- market context features
    local_median_price      FLOAT,
    local_median_dom        FLOAT,
    zip_price_to_income     FLOAT,
    -- macro features
    mortgage_rate_at_listing FLOAT,
    cpi_shelter_at_listing  FLOAT,
    -- target
    list_price              FLOAT,
    sale_price              FLOAT        -- null if not yet sold
);

-- gold.market_momentum (per zip, updated weekly)
CREATE TABLE gold.market_momentum (
    zip_code                STRING,
    snapshot_week           DATE,
    dom_trend_4wk           FLOAT,       -- change in median DOM over 4 weeks
    price_cut_trend_4wk     FLOAT,       -- change in % listings with cuts
    new_listing_trend_4wk   FLOAT,       -- % change in new listings
    price_trend_4wk         FLOAT,       -- % change in median list price
    momentum_score          FLOAT,       -- composite score (-1 to +1)
    momentum_label          STRING       -- 'heating', 'stable', 'cooling'
);
```

---

## 6. Pipeline Design

### 6.1 Airflow DAG Structure

```
housing-platform/
├── dags/
│   ├── ingest_listings_daily.py
│   ├── ingest_fred_weekly.py
│   ├── ingest_census_annual.py
│   ├── ingest_walkscore.py
│   ├── run_dbt_silver.py
│   ├── run_dbt_gold.py
│   ├── retrain_price_model.py
│   └── data_quality_checks.py
├── plugins/
│   └── operators/
│       ├── gcs_operator.py         # upload raw JSON to GCS
│       ├── delta_merge_operator.py # upsert into bronze Delta tables
│       └── dbt_operator.py         # thin wrapper around dbt CLI
├── ingestion/
│   ├── zillow_client.py
│   ├── fred_client.py
│   ├── census_client.py
│   └── walkscore_client.py
└── config/
    ├── metros.yaml                  # list of target metros and zips
    └── fred_series.yaml             # FRED series to pull
```

### 6.2 Ingest → Bronze Pattern (All Sources)

Every ingestion DAG follows the same pattern:

```
Task 1: fetch_raw_data
  → call API, save raw JSON/CSV to GCS at
    gs://housing-platform-raw/{source}/{YYYY}/{MM}/{DD}/batch_{id}.json

Task 2: validate_raw_schema
  → check required keys exist, non-null counts, value ranges
  → fail loudly if data quality below threshold

Task 3: load_to_bronze_delta
  → read from GCS, cast to Bronze schema
  → MERGE INTO bronze.{table} ON ingestion_id
    (idempotent — safe to re-run)

Task 4: log_ingest_metadata
  → write to pipeline_runs audit table:
    (dag_id, run_id, source, rows_ingested, ingested_at, status)
```

### 6.3 dbt Project Structure

```
dbt/
├── models/
│   ├── staging/
│   │   ├── stg_listings.sql          # JSON extraction from bronze
│   │   ├── stg_fred_series.sql
│   │   └── stg_census_acs.sql
│   ├── silver/
│   │   ├── silver_listings.sql       # cleaned, typed, geo-enriched
│   │   ├── silver_zip_demographics.sql
│   │   └── silver_mortgage_rates.sql
│   └── gold/
│       ├── gold_zip_market_metrics.sql
│       ├── gold_metro_affordability.sql
│       ├── gold_listing_features.sql
│       └── gold_market_momentum.sql
├── tests/
│   ├── not_null_listing_id.sql
│   ├── accepted_values_momentum_label.sql
│   ├── unique_zip_snapshot.sql
│   └── assert_affordability_ratios_positive.sql
├── macros/
│   ├── generate_surrogate_key.sql
│   └── forward_fill.sql              # for FRED series gaps
└── dbt_project.yml
```

### 6.4 CDC Strategy for Listings

Listings change over time (price cuts, status changes). We handle this with a fingerprint-based merge:

```sql
-- Fingerprint = hash of (zpid, list_price, listing_status, days_on_market)
-- If fingerprint changes → MERGE updates the record + logs the delta

MERGE INTO silver.listings AS target
USING staging.stg_listings AS source
ON target.zpid = source.zpid
WHEN MATCHED AND target.fingerprint != source.fingerprint THEN
  UPDATE SET
    target.list_price = source.list_price,
    target.listing_status = source.listing_status,
    target.days_on_market = source.days_on_market,
    target.price_cuts = source.price_cuts,
    target.fingerprint = source.fingerprint,
    target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (listing_id, zpid, ..., fingerprint, updated_at)
  VALUES (source.listing_id, source.zpid, ..., source.fingerprint, CURRENT_TIMESTAMP())
```

A separate `listing_price_history` table appends a row every time a price change is detected — this enables the price trend charts in the dashboard.

---

## 7. ML Layer Design

### 7.1 Price Estimator

**Goal:** Given property features + location + macro context, predict list price.

**Model:** XGBoost Regressor (start here; compare vs. LightGBM, Ridge as baselines)

**Feature Groups:**
- Property: sqft, beds, baths, year_built, property_type, lot_size
- Location: walk_score, transit_score, school_rating, lat/lon encoded (geohash)
- Market context: local_median_price, local_median_dom, zip_price_to_income
- Macro: mortgage_rate_at_listing, cpi_shelter_at_listing
- Time: month_of_year (seasonal signal), year

**Training data:** Gold `listing_features` table, filtered to sold listings (have sale_price)

**Validation strategy:**
- Temporal split: train on listings before cutoff date, test on after
  (not random split — avoids data leakage from future market conditions)
- Metrics: RMSE, MAPE, median absolute error

**MLflow tracking:**
```python
with mlflow.start_run():
    mlflow.log_params({"model": "xgboost", "n_estimators": 500, ...})
    mlflow.log_metrics({"rmse": rmse, "mape": mape})
    mlflow.sklearn.log_model(model, "price_estimator")
```

**Retraining cadence:** Weekly, triggered by Airflow DAG after dbt Gold run

### 7.2 Market Momentum Classifier

**Goal:** Label each zip as "heating", "stable", or "cooling" based on recent market signals.

**Approach:** Rule-based scoring (no training needed) using Gold `zip_market_metrics`:

```python
def compute_momentum_score(row):
    score = 0.0
    # DOM trending down = heating (+)
    score += -0.4 * normalize(row.dom_trend_4wk)
    # Price cuts trending down = heating (+)
    score += -0.3 * normalize(row.price_cut_trend_4wk)
    # List price trending up = heating (+)
    score +=  0.2 * normalize(row.price_trend_4wk)
    # New listings trending up = mild cooling signal (-)
    score += -0.1 * normalize(row.new_listing_trend_4wk)
    return score  # range: -1 (cooling) to +1 (heating)

def label(score):
    if score > 0.25: return "heating"
    if score < -0.25: return "cooling"
    return "stable"
```

This is implemented as a Gold dbt model + Python post-processing step in Airflow.

### 7.3 Affordability Index

**Goal:** Track how affordable housing is in each metro over time as mortgage rates change.

**Formula:**
```
monthly_payment = (median_price * 0.8 * monthly_rate) / (1 - (1+r)^-360)
mortgage_burden = monthly_payment / (median_income / 12)

affordability_tier:
  < 28%  → "affordable"    (standard threshold)
  28-36% → "stretched"
  > 36%  → "unaffordable"
```

This is a pure dbt SQL model in Gold — no ML required, but it's a powerful business metric.

---

## 8. Dashboard & Serving Layer

### 8.1 Recommended Tool: Evidence.dev

Evidence.dev is a modern open-source BI tool that renders dashboards from Markdown + SQL. It's a strong portfolio differentiator vs. the more common Tableau/Superset. Alternatively, Apache Superset works great and you have familiarity from the reference project.

### 8.2 Dashboard Pages

**Page 1: National Housing Affordability Map**
- Choropleth map colored by mortgage_burden_pct
- Time slider: animate affordability change 2020–present
- Metric callouts: national median price, current 30yr rate, YoY price change
- Source: `gold.metro_affordability`

**Page 2: Market Temperature**
- Map of zip codes colored by momentum_label (red=heating, blue=cooling, grey=stable)
- Filter by metro
- Sidebar: top 10 heating zips, top 10 cooling zips
- Source: `gold.market_momentum`

**Page 3: Metro Deep-Dive**
- Dropdown: select metro
- Time series: median list price, days on market, mortgage rate (dual axis)
- Bar chart: price tier distribution
- Table: neighborhood breakdown within metro
- Source: `gold.zip_market_metrics` + `silver.mortgage_rates`

**Page 4: Price Prediction Tool** *(interactive)*
- User inputs: beds, baths, sqft, zip code, property type
- Calls model endpoint (Flask API on Cloud Run)
- Returns: predicted price, ±confidence interval, "similar listings" table
- Source: ML model + `gold.listing_features`

**Page 5: Neighborhood Comparison**
- Side-by-side: two zips across all features
  (price, affordability, school rating, walk score, DOM, income)
- Source: JOIN across multiple Gold tables

### 8.3 Serving Architecture

For v1 (local/dev):
```
DuckDB reading from local Parquet files (Gold layer exported)
→ Evidence.dev running locally
```

For v2 (deployed):
```
Snowflake or BigQuery (Gold tables synced here)
→ Evidence.dev or Superset deployed on Cloud Run
→ Public URL
```

---

## 9. Infrastructure & DevOps

### 9.1 Local Development Setup

```
Docker Compose services:
  - airflow-webserver
  - airflow-scheduler
  - airflow-worker
  - postgres (Airflow metadata DB)
  - minio (local S3 emulation — mirrors GCS API)
```

All Airflow DAGs should work locally against MinIO + DuckDB before being promoted to GCS + Snowflake. This is the dev/prod parity pattern.

### 9.2 Cloud Setup (GCP)

```
GCP Project: housing-platform-{your-id}
├── GCS Bucket: housing-platform-raw/       (raw ingestion)
├── GCS Bucket: housing-platform-delta/     (Delta table storage)
├── Cloud Run: airflow-api (DAG trigger endpoint)
├── Cloud Run: price-model-api (Flask model serving)
├── Cloud Scheduler: cron triggers → Cloud Run
└── Artifact Registry: Docker images for pipeline containers
```

Snowflake (free trial → $0 student):
```
Database: HOUSING_PLATFORM
  ├── Schema: BRONZE
  ├── Schema: SILVER
  ├── Schema: GOLD
  └── Schema: ML_FEATURES
```

### 9.3 Repo Structure

```
housing-platform/
├── ingestion/           # Python API clients
├── dags/                # Airflow DAGs
├── dbt/                 # dbt project (transformations)
├── ml/                  # Training scripts, MLflow setup
│   ├── train_price_model.py
│   ├── score_momentum.py
│   └── mlflow_config.py
├── dashboard/           # Evidence.dev or Superset config
├── tests/               # Unit tests for ingest clients, dbt tests
├── infra/               # Terraform or docker-compose files
│   ├── docker-compose.yml
│   └── terraform/       # GCP resources (optional)
├── scripts/             # One-off utilities (backfills, etc.)
├── .env.example         # API keys template (never commit real keys)
├── requirements.txt
├── Makefile             # shortcuts: make ingest, make dbt-run, etc.
└── README.md
```

### 9.4 CI/CD (GitHub Actions)

```yaml
# .github/workflows/pipeline.yml
# Triggers on push to main:
#   1. Run dbt compile (catches SQL syntax errors)
#   2. Run dbt test on staging environment
#   3. Run Python unit tests (ingest clients, feature engineering)
#   4. Build Docker image, push to Artifact Registry
#   5. Deploy to Cloud Run (if tests pass)
```

---

## 10. Project Phases — Step-by-Step Build Plan

---

### PHASE 1 — Foundation (Week 1–2)
*Goal: Get one data source flowing end-to-end into a queryable table.*

#### Step 1.1 — Repo & Environment Setup
- [ ] Create GitHub repo: `housing-platform`
- [ ] Set up Python virtual environment (`pyenv` + `venv`)
- [ ] Create `requirements.txt`:
  `airflow, dbt-core, dbt-snowflake, delta-rs, pandas, boto3, requests, python-dotenv`
- [ ] Create `.env.example` with placeholders for all API keys
- [ ] Write `Makefile` with `make setup`, `make test`, `make dbt-run` targets
- [ ] Set up pre-commit hooks (black, flake8)
- [ ] Initialize dbt project: `dbt init housing_platform`
- [ ] **Commit:** initial repo scaffold

#### Step 1.2 — FRED API Ingest (Simplest Source First)
- [ ] Register for FRED API key (free, instant): https://fred.stlouisfed.org/docs/api/api_key.html
- [ ] Write `ingestion/fred_client.py`:
  - Function: `fetch_series(series_id, start_date, end_date) → pd.DataFrame`
  - Pull: MORTGAGE30US, MSPUS, HOUST, CUUR0000SAH, CSUSHPISA
- [ ] Write unit tests for the client (mock the HTTP call)
- [ ] Write `scripts/backfill_fred.py` to pull 5 years of history
- [ ] Save raw output to `data/raw/fred/` locally (JSON)
- [ ] **Commit:** FRED ingest client + tests

#### Step 1.3 — Local MinIO + First Bronze Table
- [ ] Create `infra/docker-compose.yml` with MinIO service
- [ ] Write `ingestion/storage_client.py` — thin wrapper around boto3 for GCS/MinIO
- [ ] Update fred_client to upload raw JSON to MinIO: `housing-platform-raw/fred/{date}/`
- [ ] Install delta-rs: `pip install deltalake`
- [ ] Write `ingestion/bronze_loader.py`:
  - Reads raw JSON from object storage
  - Writes to Bronze Delta table at `data/delta/bronze/fred_series_raw/`
- [ ] Verify: run `duckdb` and `SELECT * FROM delta_scan('data/delta/bronze/fred_series_raw/')` returns data
- [ ] **Commit:** local MinIO + bronze FRED table working

#### Step 1.4 — First dbt Model (Silver FRED)
- [ ] Configure `profiles.yml` for DuckDB locally, Snowflake for production
- [ ] Write `dbt/models/staging/stg_fred_series.sql` — light cleanup
- [ ] Write `dbt/models/silver/silver_mortgage_rates.sql`:
  - Pivot series_id rows into columns (one row per date)
  - Forward-fill gaps (FRED doesn't publish every day)
- [ ] Add schema.yml with `not_null`, `unique` dbt tests
- [ ] Run `dbt run` + `dbt test` — all pass
- [ ] **Commit:** first dbt silver model passing tests

#### Step 1.5 — Airflow Local Setup
- [ ] Add Airflow to docker-compose
- [ ] Write `dags/ingest_fred_weekly.py`:
  - Task 1: `PythonOperator` → fetch FRED data
  - Task 2: `PythonOperator` → upload to MinIO
  - Task 3: `PythonOperator` → load to bronze Delta table
  - Task 4: `BashOperator` → `dbt run --select silver_mortgage_rates`
- [ ] Test: trigger DAG manually in Airflow UI, verify all tasks green
- [ ] **Commit:** Airflow + FRED DAG working end-to-end

**Phase 1 Deliverable:** A fully automated pipeline that fetches FRED macro data weekly, stores it in object storage, loads to a Bronze Delta table, and transforms it into a clean Silver table via dbt — all running in Docker locally.

---

### PHASE 2 — Core Listings Pipeline (Week 3–4)
*Goal: Add the primary housing listings data source.*

#### Step 2.1 — Zillow API Setup
- [ ] Sign up for RapidAPI and subscribe to Zillow API (free tier)
- [ ] Explore endpoints: test in Postman or `requests` to understand response schema
- [ ] Write `ingestion/zillow_client.py`:
  - `search_listings(zip_code, listing_type) → list[dict]`
  - `get_property_detail(zpid) → dict`
  - Handle pagination, rate limiting (add `time.sleep` between calls)
- [ ] Write unit tests with a fixture (saved JSON response as test data)
- [ ] **Commit:** Zillow client + tests

#### Step 2.2 — Listings Bronze Table + CDC
- [ ] Write Bronze schema for `listings_raw` (raw JSON blob + metadata)
- [ ] Write `ingestion/cdc.py`:
  - `compute_fingerprint(zpid, price, status, dom) → str` (MD5 hash)
  - `get_changed_listings(new_df, existing_df) → pd.DataFrame`
    (returns only rows where fingerprint changed or zpid is new)
- [ ] Integrate CDC into the listings ingest script
- [ ] Write `scripts/seed_metros.py` — populate `config/metros.yaml` with target zips
  (start with 3 metros: e.g., Austin TX, Phoenix AZ, Raleigh NC)
- [ ] **Commit:** CDC implementation + listings bronze

#### Step 2.3 — Silver Listings Model (Key dbt Model)
- [ ] Write `dbt/models/staging/stg_listings.sql` — extract typed fields from JSON
- [ ] Write `dbt/models/silver/silver_listings.sql`:
  - Clean and cast all fields
  - Derive `price_per_sqft`
  - Join to HUD crosswalk for `metro_cbsa`
  - Deduplicate on `(zpid, listing_date)` keeping latest fingerprint
- [ ] Write `dbt/models/silver/silver_listing_price_history.sql`:
  - Append-only log of every price change
- [ ] Add dbt tests: not_null, unique, accepted_values for property_type
- [ ] `dbt run && dbt test` — all pass
- [ ] **Commit:** silver listings + price history models

#### Step 2.4 — Census API Ingest
- [ ] Write `ingestion/census_client.py`:
  - `fetch_acs_table(table_id, zip_codes, year) → pd.DataFrame`
  - Pull B19013, B01003, B25003, B25064
- [ ] Bronze table + dbt Silver model: `silver_zip_demographics`
- [ ] **Commit:** Census ingest + silver demographics

#### Step 2.5 — Listings Airflow DAG
- [ ] Write `dags/ingest_listings_daily.py`:
  - Parameterize by metro list from config
  - Implement retry logic (API calls fail sometimes)
  - Send Slack/email alert on failure (Airflow built-in)
- [ ] Test with one metro manually, then enable for all 3
- [ ] **Commit:** listings DAG

**Phase 2 Deliverable:** Daily automated listing ingestion with CDC for 3 metros, flowing into clean Silver tables in dbt. You can now answer: "What are the current listings in Austin, and how have prices changed?"

---

### PHASE 3 — Gold Layer & Business Metrics (Week 5–6)
*Goal: Build the analytics-ready Gold tables that power the dashboard and ML.*

#### Step 3.1 — Gold: Zip Market Metrics
- [ ] Write `gold/gold_zip_market_metrics.sql`:
  - Aggregate Silver listings by (zip_code, snapshot_week)
  - Compute: median_list_price, median_dom, pct_with_price_cut, new_listings_7d
- [ ] Add dbt tests: row counts, expected ranges
- [ ] **Commit**

#### Step 3.2 — Gold: Market Momentum
- [ ] Write `gold/gold_market_momentum.sql`:
  - 4-week rolling window on zip_market_metrics
  - Compute trend columns (LAG functions)
- [ ] Write `ml/score_momentum.py`:
  - Apply momentum scoring formula
  - Write results back to `gold.market_momentum`
- [ ] Add to dbt as a Python model (dbt 1.3+ supports Python models)
- [ ] **Commit**

#### Step 3.3 — Gold: Affordability Index
- [ ] Write `gold/gold_metro_affordability.sql`:
  - JOIN silver_listings (median price by CBSA) + silver_zip_demographics
    (median income) + silver_mortgage_rates (current rate)
  - Compute monthly_payment, mortgage_burden_pct, affordability_tier
- [ ] **Commit**

#### Step 3.4 — Gold: ML Feature Table
- [ ] Write `gold/gold_listing_features.sql`:
  - JOIN silver_listings + silver_zip_demographics + walk_score data
  - Add macro features via join on listing_date to silver_mortgage_rates
- [ ] Validate: check feature completeness, null rates per column
- [ ] **Commit**

#### Step 3.5 — Data Quality Layer
- [ ] Install Great Expectations: `pip install great_expectations`
- [ ] Write expectations suite for `gold_listing_features`:
  - Price must be > 0
  - Beds between 0 and 20
  - Lat/lon within US bounding box
  - No duplicate listing_ids
- [ ] Integrate GE checkpoint into Airflow (run after dbt Gold)
- [ ] **Commit:** full data quality layer

**Phase 3 Deliverable:** All four Gold tables populated and tested. You can now directly answer the core business questions: affordability by metro, market temperature by zip, ML-ready feature set.

---

### PHASE 4 — ML Models (Week 7–8)

#### Step 4.1 — MLflow Setup
- [ ] Set up MLflow tracking server (local SQLite for dev, or MLflow on Cloud Run)
- [ ] Configure `ml/mlflow_config.py`

#### Step 4.2 — Price Estimator (v1)
- [ ] Write `ml/train_price_model.py`:
  - Load `gold.listing_features` from DuckDB/Snowflake
  - Temporal train/test split
  - Baseline: median price by zip (sanity check)
  - Model 1: Ridge Regression
  - Model 2: XGBoost
  - Log all metrics + artifacts to MLflow
- [ ] Evaluate: RMSE, MAPE, residuals plot
- [ ] Register best model in MLflow Model Registry
- [ ] **Commit**

#### Step 4.3 — Price Model Serving API
- [ ] Write `ml/serve.py` — Flask API:
  - `POST /predict` → accepts feature JSON, returns predicted price + confidence interval
- [ ] Write `Dockerfile` for the serve container
- [ ] Test locally with `curl`
- [ ] Deploy to Cloud Run
- [ ] **Commit:** model API deployed

#### Step 4.4 — Retrain DAG
- [ ] Write `dags/retrain_price_model.py`:
  - Task 1: export fresh Gold features
  - Task 2: run training script
  - Task 3: evaluate vs. current production model
  - Task 4: promote if RMSE improves (automatic)
  - Task 5: redeploy Cloud Run service with new model artifact
- [ ] **Commit**

**Phase 4 Deliverable:** A trained XGBoost price estimator, served via API, with automated weekly retraining. MLflow dashboard shows experiment history.

---

### PHASE 5 — Dashboard & Deployment (Week 9–10)

#### Step 5.1 — Evidence.dev Setup
- [ ] Install Evidence: `npm create evidence@latest`
- [ ] Configure DuckDB data source (pointing to Gold Parquet files)
- [ ] **Commit:** Evidence scaffold

#### Step 5.2 — Build Dashboard Pages
- [ ] Page 1: Affordability Map (choropleth from `gold.metro_affordability`)
- [ ] Page 2: Market Temperature (zip-level momentum map)
- [ ] Page 3: Metro Deep-Dive (time series for user-selected metro)
- [ ] Page 4: Price Prediction (form → calls Cloud Run API)
- [ ] Page 5: Neighborhood Comparison (two-zip side-by-side)
- [ ] **Commit:** all 5 pages

#### Step 5.3 — Cloud Deployment
- [ ] Set up GCP project (or use free tier)
- [ ] Migrate from MinIO to GCS (change `STORAGE_ENDPOINT` env var)
- [ ] Migrate from DuckDB to Snowflake (change dbt profile)
- [ ] Deploy Evidence.dev to Cloud Run or Vercel
- [ ] Set up Cloud Scheduler to trigger Airflow DAGs on schedule
- [ ] Verify end-to-end pipeline runs in cloud
- [ ] **Commit:** cloud deployment working

#### Step 5.4 — README & Documentation
- [ ] Write thorough `README.md`:
  - Architecture diagram
  - How to run locally (one `make setup && make run` command)
  - Screenshots of dashboard
  - Link to live demo
- [ ] Record a 2-minute Loom walkthrough of the system
- [ ] **Commit:** final

**Phase 5 Deliverable:** Live deployed dashboard at a public URL. Full README. Loom demo. Portfolio-ready.

---

### PHASE 6 — Stretch Goals (As Time Allows)

| Enhancement | Complexity | Impact |
|---|---|---|
| Add Redfin data source as secondary | Medium | More listing coverage |
| Streaming CDC with Pub/Sub for new listings | High | Real resume differentiator |
| Walk Score + School data ingestion | Low | Better ML features |
| dbt Semantic Layer for self-serve queries | Medium | Shows dbt depth |
| Terraform for all GCP infra | Medium | IaC best practice |
| GitHub Actions CI/CD pipeline | Low | Shows DevOps awareness |
| Price alert notifications (email/webhook) | Medium | End-user feature |
| Historical backfill to 2019 (pre-COVID baseline) | Low | Better time-series analysis |

---

## 11. Tech Stack Summary

| Layer | Tool | Rationale |
|---|---|---|
| Orchestration | **Apache Airflow** | Industry standard; already in your portfolio |
| Object Storage (local) | **MinIO** | S3-compatible, runs in Docker |
| Object Storage (cloud) | **GCS** | Already in your existing projects |
| Table Format | **Delta Lake** (delta-rs) | ACID transactions on files, no Spark needed |
| Transformation | **dbt** | Already in your Snowflake warehouse project |
| Data Warehouse | **DuckDB** (dev) / **Snowflake** (prod) | DuckDB = zero cost dev; Snowflake = production |
| ML Framework | **scikit-learn + XGBoost** | Industry standard for tabular data |
| ML Tracking | **MLflow** | Experiment tracking, model registry |
| Model Serving | **Flask on Cloud Run** | Simple, cheap, deployable |
| Dashboard | **Evidence.dev** | Modern, markdown-based, great for portfolios |
| Data Quality | **dbt tests + Great Expectations** | Shows production awareness |
| Containerization | **Docker Compose** | Local parity with cloud |
| Cloud | **GCP** | Already familiar from your projects |
| CI/CD | **GitHub Actions** | Free for public repos |

---

## 12. Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Zillow API rate limits / cost | Medium | High | Cache aggressively; start with 3 metros; use free Redfin CSV as backup |
| Census/FRED data staleness | Low | Medium | ACS is annual; always note vintage year in schema |
| Snowflake credit burn | Medium | Medium | Use DuckDB for all dev; Snowflake only for final production demo |
| Schema changes in Zillow API response | Medium | Medium | Store raw JSON in Bronze — schema changes only break Silver, not raw |
| Cloud Run cold start latency for model API | Low | Low | Add a warm-up endpoint; acceptable for portfolio demo |
| Airflow complexity for solo project | High | Medium | Use Airflow Lite / Astro CLI for simpler local setup |
| GCS egress costs | Low | Low | Keep gold Parquet exports small; Evidence.dev reads locally |

---

*Document Version: 1.0*
*Last Updated: 2026*
*Author: Alex (UC Berkeley, MIDS/Haas)*
