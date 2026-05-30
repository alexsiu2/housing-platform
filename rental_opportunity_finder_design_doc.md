# Rental Property Opportunity Finder
## Technical Design Document v2.0

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Goals & Success Metrics](#2-goals--success-metrics)
3. [Architecture Overview](#3-architecture-overview)
4. [Data Sources](#4-data-sources)
5. [Data Model & Schema Design](#5-data-model--schema-design)
6. [Opportunity Score Design](#6-opportunity-score-design)
7. [Pipeline Design](#7-pipeline-design)
8. [Dashboard & Serving Layer](#8-dashboard--serving-layer)
9. [Infrastructure & DevOps](#9-infrastructure--devops)
10. [Project Phases — Step-by-Step Build Plan](#10-project-phases--step-by-step-build-plan)
11. [Tech Stack Summary](#11-tech-stack-summary)
12. [Risks & Mitigations](#12-risks--mitigations)

---

## 1. Project Overview

**Project Name:** Rental Property Opportunity Finder (RPOF)

**One-Line Summary:** A production-grade data engineering pipeline that ingests entirely free, open housing and demographic data, transforms it through a medallion lakehouse architecture, and surfaces a ranked zip-code opportunity score to identify the best US markets for rental property investment.

**Motivation:** Tools like RentCast and PropertyScout360 solve this problem as paid SaaS products. This project builds the same analytical output from scratch using only open data — demonstrating that a well-architected data pipeline can replicate commercial-grade market intelligence at zero data cost. The secondary motivation is personal: the dashboard is a genuinely useful tool for evaluating real estate investment decisions.

**The core question the platform answers:**
> "Which US zip codes right now offer the best combination of rent growth, affordable entry price for investors, sustainable renter demand, and population growth signals?"

**What makes this portfolio-worthy:**
- End-to-end pipeline with real orchestration (Airflow), not a notebook
- Multi-source data fusion from 5 open data sources requiring schema reconciliation
- Production dbt medallion architecture (Bronze → Silver → Gold) with data quality tests
- Explainable composite scoring model — no black box, every score is decomposable
- Live deployed dashboard with a public URL
- $0 data cost — entirely open/free data sources
- Personally usable: a real investment research tool, not a toy

---

## 2. Goals & Success Metrics

### Primary Goals

| Goal | Definition of Done |
|------|-------------------|
| Ingest Zillow ZORI (rent index) by zip | Monthly CSV download → Bronze, covering full history |
| Ingest Zillow ZHVI (home value index) by zip | Monthly CSV download → Bronze, covering full history |
| Ingest FRED macro signals | Weekly API fetch of 4 key series → Bronze |
| Ingest Census ACS demographics by zip | Annual API fetch → Bronze |
| Ingest BLS job growth by metro | Quarterly API fetch → Bronze |
| Build medallion lakehouse | Bronze/Silver/Gold with dbt, all tests passing |
| Compute opportunity score | Per-zip composite score with 5 sub-metrics, updated monthly |
| Live dashboard | Deployed Evidence.dev instance with public URL |
| Data quality layer | dbt tests on all Gold tables, zero silent failures |

### Stretch Goals

| Goal | Notes |
|------|-------|
| HUD Fair Market Rents integration | Adds a second rent data source for cross-validation |
| Cap rate estimator | Requires property tax data by county — complex but valuable |
| Email alert system | Notify when a zip's opportunity score crosses a threshold |
| Redfin data integration | Adds listing-level data for deeper zip analysis |
| Terraform for GCP infra | IaC polish for the resume |
| GitHub Actions CI/CD | Auto-deploy on push to main |

---

## 3. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                         INGESTION LAYER                               │
│                                                                        │
│  [Zillow ZORI CSV]  [Zillow ZHVI CSV]  [FRED API]  [Census API]       │
│                              [BLS API]                                 │
│                                   │                                    │
│              Python ingest scripts (called by Airflow DAGs)           │
└───────────────────────────────────┬──────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│                          STORAGE LAYER                                 │
│                                                                        │
│   GCS Bucket: rental-opportunity-raw/                                 │
│     ├── zillow/zori/YYYY/MM/Metro_zhvi_uc_sfrcondo_tier_0.33_0.67... │
│     ├── zillow/zhvi/YYYY/MM/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67...   │
│     ├── fred/YYYY/MM/DD/*.json                                        │
│     ├── census/YYYY/*.json                                            │
│     └── bls/YYYY/QQ/*.json                                            │
│                                                                        │
│   BRONZE Delta Tables (raw, append-only, schema-on-read)              │
│     ├── bronze.zori_raw                                               │
│     ├── bronze.zhvi_raw                                               │
│     ├── bronze.fred_series_raw      ← already built ✅                │
│     ├── bronze.census_acs_raw                                         │
│     └── bronze.bls_metro_raw                                          │
└───────────────────────────────────┬──────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    TRANSFORMATION LAYER (dbt)                          │
│                                                                        │
│   SILVER (cleaned, typed, deduplicated, joined)                       │
│     ├── silver.rent_index          (ZORI by zip, monthly)             │
│     ├── silver.home_values         (ZHVI by zip, monthly)             │
│     ├── silver.mortgage_rates      (FRED, forward-filled) ← built ✅  │
│     ├── silver.zip_demographics    (Census ACS, annual)               │
│     └── silver.metro_job_growth    (BLS, quarterly)                   │
│                                                                        │
│   GOLD (business metrics, investment analytics)                       │
│     ├── gold.zip_investment_metrics    (all metrics per zip)          │
│     └── gold.zip_opportunity_score     (ranked composite score)       │
└──────────────────────┬────────────────────────────────────────────────┘
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
┌────────────────────┐  ┌───────────────────────────────────────────┐
│   SCORING LAYER    │  │            SERVING LAYER                   │
│                    │  │                                             │
│  score.py          │  │  DuckDB (dev) / Snowflake (prod)           │
│  Reads Gold metrics│  │  ← queried by Evidence.dev dashboard       │
│  Applies weighting │  │                                             │
│  Writes scores     │  │  Dashboard pages:                          │
│  back to Gold      │  │  - National opportunity map                │
│                    │  │  - Zip deep-dive                           │
└────────────────────┘  │  - Metro comparison                        │
                        │  - Custom screener                         │
                        └───────────────────────────────────────────┘
                                        │
                    ┌───────────────────┴──────────────────────┐
                    ▼                                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION (Airflow)                             │
│                                                                        │
│   DAGs:                                                                │
│   - ingest_zillow_monthly        (1st of month — downloads CSVs)     │
│   - ingest_fred_weekly           (Mondays) ← already built ✅         │
│   - ingest_census_annual         (January — ACS release)             │
│   - ingest_bls_quarterly         (quarterly BLS release)             │
│   - run_dbt_silver               (after each ingest DAG)             │
│   - run_dbt_gold                 (after silver runs)                  │
│   - compute_opportunity_scores   (after gold runs)                   │
│   - data_quality_checks          (after scoring)                     │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 4. Data Sources

### 4.1 Zillow Research CSVs — ZORI (Rent Index)

- **What:** Zillow Observed Rent Index — smoothed monthly rent estimate per zip code, weighted to represent the full rental housing stock (not just listed units)
- **How:** Direct CSV download from `zillow.com/research/data/` — no API key, no rate limits, completely free
- **File:** `Zip_zori_uc_sfrcondomfr_sm_month.csv`
- **Coverage:** ~2,500 zip codes nationally, monthly data back to 2015
- **Key columns:** `RegionName` (zip), `RegionID`, `MsaName`, then one column per month (`2015-01-31`, `2015-02-28`, etc.) — wide format, must be melted to long
- **Cadence:** Released monthly, usually the first week of the month
- **What we use it for:** Current rent level per zip, rent growth YoY, rent growth 3yr

### 4.2 Zillow Research CSVs — ZHVI (Home Value Index)

- **What:** Zillow Home Value Index — smoothed monthly estimate of typical home value per zip code (middle tier, single family + condo)
- **How:** Same download page as ZORI — free CSV
- **File:** `Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv`
- **Coverage:** ~26,000 zip codes nationally, monthly data back to 2000
- **Key columns:** Same wide format as ZORI — `RegionName`, `RegionID`, `StateName`, `Metro`, then monthly columns
- **Cadence:** Monthly
- **What we use it for:** Entry price for investors, price-to-rent ratio, home value growth

### 4.3 FRED API — Macro Context

- **What:** Macro signals that affect rental investment returns
- **How:** FRED API — already built in `ingestion/fred_client.py` ✅
- **Series:**
  - `MORTGAGE30US` — 30-year fixed rate (affects investor financing cost)
  - `CUUR0000SAH` — CPI shelter (national rent inflation benchmark)
  - `UNRATE` — National unemployment rate (economic health signal)
  - `HOUST` — Housing starts (supply signal)
- **Cadence:** Weekly

### 4.4 Census ACS API — Demographics by Zip

- **What:** Zip-level demographic and economic data from the American Community Survey
- **How:** Census API — free, no key required for low volume
- **Tables:**
  - `B19013_001E` — Median household income (renter affordability denominator)
  - `B01003_001E` — Total population
  - `B25003_003E` — Renter-occupied units (rental market size signal)
  - `B25003_001E` — Total occupied housing units (to compute renter ratio)
  - `B23025_005E` — Unemployed population (local employment signal)
- **Cadence:** Annual (ACS 5-year estimates, released each December)
- **What we use it for:** Renter affordability ratio, rental demand proxy, population base

### 4.5 BLS API — Job Growth by Metro

- **What:** Quarterly employment levels and year-over-year job growth by Metropolitan Statistical Area (MSA)
- **How:** Bureau of Labor Statistics public API — free
- **Series:** Quarterly Census of Employment and Wages (QCEW), total nonfarm employment by MSA
- **Cadence:** Quarterly (released ~5 months after the reference quarter)
- **What we use it for:** Job growth is the strongest predictor of sustained rental demand. A metro adding jobs attracts workers who need housing.
- **Join key:** MSA code → zip via HUD USPS crosswalk

### 4.6 HUD USPS Crosswalk (Reference Table)

- **What:** Maps zip codes to CBSA/MSA codes — the critical join key that connects zip-level data (Zillow, Census) to metro-level data (BLS, FRED metro series)
- **How:** HUD website quarterly download — free CSV
- **Cadence:** Quarterly update, but changes rarely affect major metros

---

## 5. Data Model & Schema Design

### 5.1 Bronze Layer (Raw — No Transforms)

```sql
-- bronze.zori_raw
-- Wide-format CSV ingested as-is, one row per zip per download run
CREATE TABLE bronze.zori_raw (
    region_name         STRING,     -- zip code
    region_id           STRING,     -- Zillow internal ID
    msa_name            STRING,     -- metro area name
    state_name          STRING,
    month               DATE,       -- after melting from wide to long
    rent_index          FLOAT,
    ingested_at         TIMESTAMP,
    source_file         STRING      -- filename of the CSV that produced this row
);

-- bronze.zhvi_raw
CREATE TABLE bronze.zhvi_raw (
    region_name         STRING,     -- zip code
    region_id           STRING,
    metro               STRING,
    state_name          STRING,
    month               DATE,       -- after melting wide to long
    home_value_index    FLOAT,
    ingested_at         TIMESTAMP,
    source_file         STRING
);

-- bronze.fred_series_raw  ← already exists ✅
-- bronze.census_acs_raw   ← schema defined in Section 4.4

-- bronze.bls_metro_raw
CREATE TABLE bronze.bls_metro_raw (
    msa_code            STRING,
    msa_name            STRING,
    reference_quarter   STRING,     -- e.g. '2024-Q3'
    total_employment    INT,
    yoy_employment_change INT,
    yoy_pct_change      FLOAT,
    ingested_at         TIMESTAMP
);
```

### 5.2 Silver Layer (Cleaned, Typed, Deduplicated)

```sql
-- silver.rent_index
-- One row per zip per month. Clean types, no duplicates.
CREATE TABLE silver.rent_index (
    zip_code            STRING,
    metro_name          STRING,
    state               STRING,
    month               DATE,
    median_rent         FLOAT,       -- ZORI value in dollars
    updated_at          TIMESTAMP
);

-- silver.home_values
-- One row per zip per month.
CREATE TABLE silver.home_values (
    zip_code            STRING,
    metro_name          STRING,
    state               STRING,
    cbsa_code           STRING,      -- joined from HUD crosswalk
    month               DATE,
    median_home_value   FLOAT,       -- ZHVI value in dollars
    updated_at          TIMESTAMP
);

-- silver.mortgage_rates  ← already exists ✅

-- silver.zip_demographics
-- One row per zip. Annual cadence.
CREATE TABLE silver.zip_demographics (
    zip_code                STRING,
    cbsa_code               STRING,
    median_household_income FLOAT,
    total_population        INT,
    renter_occupied_units   INT,
    total_occupied_units    INT,
    renter_ratio            FLOAT,   -- renter_occupied / total_occupied
    vintage_year            INT
);

-- silver.metro_job_growth
-- One row per MSA per quarter.
CREATE TABLE silver.metro_job_growth (
    msa_code                STRING,
    msa_name                STRING,
    reference_quarter       DATE,
    total_employment        INT,
    yoy_employment_change   INT,
    yoy_pct_change          FLOAT
);
```

### 5.3 Gold Layer (Investment Analytics)

```sql
-- gold.zip_investment_metrics
-- The core analytical table. One row per zip per month.
-- Every metric needed to evaluate a zip for rental investment.
CREATE TABLE gold.zip_investment_metrics (
    zip_code                    STRING,
    metro_name                  STRING,
    state                       STRING,
    cbsa_code                   STRING,
    snapshot_month              DATE,

    -- Rent metrics (from ZORI)
    median_rent                 FLOAT,   -- current monthly rent ($)
    rent_growth_1yr             FLOAT,   -- % change YoY
    rent_growth_3yr             FLOAT,   -- % change over 3 years
    annual_gross_rent           FLOAT,   -- median_rent × 12

    -- Home value metrics (from ZHVI)
    median_home_value           FLOAT,   -- current home value ($)
    home_value_growth_1yr       FLOAT,   -- % change YoY

    -- Core investment ratios
    price_to_rent_ratio         FLOAT,   -- home_value ÷ annual_gross_rent
    gross_rental_yield          FLOAT,   -- annual_gross_rent ÷ home_value × 100

    -- Renter demand signals (from Census)
    median_household_income     FLOAT,
    renter_ratio                FLOAT,   -- % of occupied units that are rented
    rent_to_income_ratio        FLOAT,   -- monthly_rent ÷ (income ÷ 12)
                                         -- below 0.30 = affordable for renters

    -- Supply/demand context
    total_population            INT,
    yoy_job_growth_pct          FLOAT,   -- from BLS (metro level, joined by CBSA)

    -- Macro context
    mortgage_rate_30yr          FLOAT,   -- from FRED (national)

    -- Data freshness
    rent_data_vintage           DATE,
    value_data_vintage          DATE,
    demographics_vintage        INT
);

-- gold.zip_opportunity_score
-- One row per zip. The final ranked output — what the dashboard shows.
CREATE TABLE gold.zip_opportunity_score (
    zip_code                    STRING PRIMARY KEY,
    metro_name                  STRING,
    state                       STRING,
    snapshot_month              DATE,

    -- Raw metrics (carried forward for decomposability)
    median_rent                 FLOAT,
    median_home_value           FLOAT,
    gross_rental_yield          FLOAT,
    price_to_rent_ratio         FLOAT,
    rent_growth_1yr             FLOAT,
    rent_to_income_ratio        FLOAT,
    renter_ratio                FLOAT,
    yoy_job_growth_pct          FLOAT,

    -- Normalized sub-scores (0–100 each, higher = better for investor)
    score_rental_yield          FLOAT,   -- high yield = high score
    score_rent_growth           FLOAT,   -- high growth = high score
    score_affordability         FLOAT,   -- low rent-to-income = high score
                                         -- (sustainable renter demand)
    score_entry_price           FLOAT,   -- low price-to-rent = high score
    score_demand_growth         FLOAT,   -- high job growth + renter ratio = high score

    -- Final composite
    opportunity_score           FLOAT,   -- weighted sum of sub-scores (0–100)
    opportunity_tier            STRING,  -- 'strong', 'moderate', 'weak'
    national_rank               INT,     -- rank among all scored zips
    metro_rank                  INT      -- rank within the zip's metro
);
```

---

## 6. Opportunity Score Design

The opportunity score is the heart of the project. It must be explainable — every zip's score should be fully decomposable into its five sub-scores, and each sub-score should trace back to a single raw metric.

### 6.1 Sub-Score Definitions

Each sub-score is computed by ranking the metric across all zips nationally and scaling to 0–100.

| Sub-Score | Raw Metric | Logic | Weight |
|---|---|---|---|
| `score_rental_yield` | `gross_rental_yield` | Higher yield = higher score. Rentals yielding 8%+ score near 100. | 30% |
| `score_rent_growth` | `rent_growth_1yr` | Higher YoY rent growth = higher score. Captures market momentum. | 25% |
| `score_affordability` | `rent_to_income_ratio` | Lower ratio = higher score. Ensures renters can actually afford it — vacant units earn nothing. | 20% |
| `score_entry_price` | `price_to_rent_ratio` | Lower ratio = higher score. Below 15 = favorable entry, above 25 = expensive. | 15% |
| `score_demand_growth` | `yoy_job_growth_pct` | Higher job growth = higher score. Jobs bring workers who rent. | 10% |

### 6.2 Composite Score Formula

```python
opportunity_score = (
    score_rental_yield   * 0.30 +
    score_rent_growth    * 0.25 +
    score_affordability  * 0.20 +
    score_entry_price    * 0.15 +
    score_demand_growth  * 0.10
)
```

### 6.3 Opportunity Tier Thresholds

```python
def assign_tier(score):
    if score >= 70: return 'strong'
    if score >= 45: return 'moderate'
    return 'weak'
```

### 6.4 Why These Weights

Rental yield is weighted highest (30%) because it directly determines cash flow. Rent growth (25%) captures appreciation potential. Renter affordability (20%) is a demand sustainability signal — a market where renters spend 50% of income on rent will see high turnover and vacancy. Entry price (15%) matters but is already partially captured by yield. Job growth (10%) is a leading indicator but is only available at metro level, not zip, so its precision is lower.

These weights are intentionally documented and defensible. In an interview you can say "I chose these weights because X" — that's a more interesting conversation than a black box model.

### 6.5 Normalization Method

Each sub-score uses percentile ranking within the current snapshot month:

```python
# Example for rental yield
df['score_rental_yield'] = (
    df['gross_rental_yield']
    .rank(pct=True)   # returns 0.0 to 1.0
    * 100             # scale to 0–100
)
```

Percentile ranking is robust to outliers (a 25% yield in an unusual zip doesn't distort everyone else's score) and produces intuitive outputs (a score of 80 means this zip beats 80% of all zips on this metric).

---

## 7. Pipeline Design

### 7.1 Airflow DAG Structure

```
rental-opportunity-finder/
├── dags/
│   ├── ingest_zillow_monthly.py        # downloads ZORI + ZHVI CSVs
│   ├── ingest_fred_weekly.py           # ← already built ✅
│   ├── ingest_census_annual.py
│   ├── ingest_bls_quarterly.py
│   ├── run_dbt_silver.py
│   ├── run_dbt_gold.py
│   ├── compute_opportunity_scores.py   # runs score.py, writes to gold
│   └── data_quality_checks.py
├── ingestion/
│   ├── fred_client.py                  # ← already built ✅
│   ├── zillow_csv_client.py            # downloads + melts Zillow CSVs
│   ├── census_client.py
│   └── bls_client.py
├── scoring/
│   └── score.py                        # opportunity score computation
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_zori.sql
│   │   │   ├── stg_zhvi.sql
│   │   │   ├── stg_census_acs.sql
│   │   │   └── stg_bls_metro.sql
│   │   ├── silver/
│   │   │   ├── silver_rent_index.sql
│   │   │   ├── silver_home_values.sql
│   │   │   ├── silver_zip_demographics.sql
│   │   │   └── silver_metro_job_growth.sql
│   │   └── gold/
│   │       ├── gold_zip_investment_metrics.sql
│   │       └── gold_zip_opportunity_score.sql
│   └── tests/
│       ├── assert_yield_positive.sql
│       ├── assert_score_between_0_100.sql
│       ├── assert_rent_not_null.sql
│       └── unique_zip_snapshot.sql
├── config/
│   └── zillow_files.yaml              # list of CSV filenames to download
└── tests/
    ├── test_zillow_client.py
    ├── test_census_client.py
    └── test_score.py
```

### 7.2 Zillow CSV Ingest Pattern

Zillow CSVs are wide format — one column per month. The ingest script melts them to long format before writing to Bronze.

```
Task 1: download_zillow_csv
  → requests.get(ZILLOW_ZORI_URL) → save to GCS
    gs://rental-opportunity-raw/zillow/zori/{YYYY}/{MM}/zori.csv

Task 2: melt_and_validate
  → pd.melt(df, id_vars=['RegionName','Metro','StateName'],
             var_name='month', value_name='rent_index')
  → filter month columns only (format: YYYY-MM-DD)
  → validate: non-null rent_index > 0, zip format valid

Task 3: load_to_bronze_delta
  → MERGE INTO bronze.zori_raw ON (region_name, month)
  → idempotent — safe to re-run without duplicating history

Task 4: log_ingest_metadata
  → rows ingested, source file, run timestamp → audit table
```

### 7.3 dbt Gold Model Logic

The key transformation in `gold_zip_investment_metrics.sql`:

```sql
WITH rent AS (
    SELECT
        zip_code,
        metro_name,
        state,
        month AS snapshot_month,
        median_rent,
        -- YoY rent growth
        (median_rent - LAG(median_rent, 12) OVER (
            PARTITION BY zip_code ORDER BY month
        )) / NULLIF(LAG(median_rent, 12) OVER (
            PARTITION BY zip_code ORDER BY month
        ), 0) * 100 AS rent_growth_1yr,
        -- 3yr rent growth
        (median_rent - LAG(median_rent, 36) OVER (
            PARTITION BY zip_code ORDER BY month
        )) / NULLIF(LAG(median_rent, 36) OVER (
            PARTITION BY zip_code ORDER BY month
        ), 0) * 100 AS rent_growth_3yr
    FROM {{ ref('silver_rent_index') }}
),

values AS (
    SELECT
        zip_code,
        cbsa_code,
        month,
        median_home_value,
        (median_home_value - LAG(median_home_value, 12) OVER (
            PARTITION BY zip_code ORDER BY month
        )) / NULLIF(LAG(median_home_value, 12) OVER (
            PARTITION BY zip_code ORDER BY month
        ), 0) * 100 AS home_value_growth_1yr
    FROM {{ ref('silver_home_values') }}
)

SELECT
    r.zip_code,
    r.metro_name,
    r.state,
    v.cbsa_code,
    r.snapshot_month,
    r.median_rent,
    r.rent_growth_1yr,
    r.rent_growth_3yr,
    r.median_rent * 12                              AS annual_gross_rent,
    v.median_home_value,
    v.home_value_growth_1yr,
    v.median_home_value / NULLIF(r.median_rent * 12, 0)
                                                    AS price_to_rent_ratio,
    (r.median_rent * 12) / NULLIF(v.median_home_value, 0) * 100
                                                    AS gross_rental_yield,
    d.median_household_income,
    d.renter_ratio,
    r.median_rent / NULLIF(d.median_household_income / 12, 0)
                                                    AS rent_to_income_ratio,
    d.total_population,
    j.yoy_pct_change                                AS yoy_job_growth_pct,
    m.rate_30yr_fixed                               AS mortgage_rate_30yr

FROM rent r
LEFT JOIN values v
    ON r.zip_code = v.zip_code AND r.snapshot_month = v.month
LEFT JOIN {{ ref('silver_zip_demographics') }} d
    ON r.zip_code = d.zip_code
LEFT JOIN {{ ref('silver_metro_job_growth') }} j
    ON v.cbsa_code = j.msa_code
    AND DATE_TRUNC('quarter', r.snapshot_month) = j.reference_quarter
LEFT JOIN {{ ref('silver_mortgage_rates') }} m
    ON r.snapshot_month = m.observation_date

WHERE r.snapshot_month = (SELECT MAX(month) FROM {{ ref('silver_rent_index') }})
```

### 7.4 Scoring Layer (score.py)

The scoring runs as a Python step after dbt Gold, not inside dbt itself, because it requires cross-zip percentile ranking which is cleaner in pandas than SQL.

```python
import duckdb
import pandas as pd

def compute_scores(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = conn.execute("""
        SELECT * FROM gold.zip_investment_metrics
        WHERE snapshot_month = (SELECT MAX(snapshot_month)
                                FROM gold.zip_investment_metrics)
        AND gross_rental_yield IS NOT NULL
        AND rent_growth_1yr IS NOT NULL
    """).df()

    # Compute percentile sub-scores (0–100)
    df['score_rental_yield']  = df['gross_rental_yield'].rank(pct=True) * 100
    df['score_rent_growth']   = df['rent_growth_1yr'].rank(pct=True) * 100
    # For affordability: lower rent-to-income is better → invert
    df['score_affordability'] = (1 - df['rent_to_income_ratio'].rank(pct=True)) * 100
    # For entry price: lower price-to-rent is better → invert
    df['score_entry_price']   = (1 - df['price_to_rent_ratio'].rank(pct=True)) * 100
    df['score_demand_growth'] = df['yoy_job_growth_pct'].rank(pct=True) * 100

    # Composite score
    df['opportunity_score'] = (
        df['score_rental_yield']  * 0.30 +
        df['score_rent_growth']   * 0.25 +
        df['score_affordability'] * 0.20 +
        df['score_entry_price']   * 0.15 +
        df['score_demand_growth'] * 0.10
    )

    # Tier and rank
    df['opportunity_tier'] = df['opportunity_score'].apply(
        lambda s: 'strong' if s >= 70 else ('moderate' if s >= 45 else 'weak')
    )
    df['national_rank'] = df['opportunity_score'].rank(ascending=False).astype(int)
    df['metro_rank'] = df.groupby('metro_name')['opportunity_score'] \
                         .rank(ascending=False).astype(int)

    return df
```

---

## 8. Dashboard & Serving Layer

### 8.1 Tool: Evidence.dev

Evidence.dev renders dashboards from Markdown + SQL. Each page is a `.md` file with embedded SQL queries. It connects directly to DuckDB locally and Snowflake in production. No separate BI server needed.

### 8.2 Dashboard Pages

**Page 1 — National Opportunity Map** *(main landing page)*

The hero view. A national choropleth map of zip codes colored by `opportunity_score`. Green = strong opportunity, red = weak.

- Filter sidebar: state, metro, min gross yield, max home value, min rent growth
- Top 50 table below the map: ranked list of best zip codes nationally
- Metric callouts: current 30yr mortgage rate, national median rent YoY change
- Source: `gold.zip_opportunity_score`

**Page 2 — Zip Deep-Dive**

User selects a zip code. Full breakdown of that zip's scores.

- Score decomposition bar chart: shows all 5 sub-scores and the composite
- Rent trend line chart: ZORI history for this zip (going back to 2015)
- Home value trend line chart: ZHVI history
- Key stats table: yield, price-to-rent, rent growth, rent-to-income, job growth
- Context: where does this zip rank nationally and within its metro?
- Source: `gold.zip_investment_metrics` (full history) + `gold.zip_opportunity_score`

**Page 3 — Metro Comparison**

Compare two metros head-to-head.

- Side-by-side: median rent, median home value, avg yield, avg score
- Distribution chart: histogram of opportunity scores within each metro
- Top 10 zip codes per metro ranked by score
- Source: `gold.zip_opportunity_score` grouped by metro

**Page 4 — Custom Screener**

The power-user tool. Filter to your personal criteria.

- Inputs: max home value ($), min gross yield (%), max price-to-rent ratio, min rent growth (%), state/metro filter
- Output: sorted table of qualifying zips with all metrics
- Export to CSV button
- Source: `gold.zip_opportunity_score`

### 8.3 Key Dashboard Design Principle

Every number on the dashboard traces back to a raw data source. The score decomposition chart on Page 2 is critical — it makes the tool trustworthy rather than magical. An investor should be able to look at a zip, see it scores 82 overall, see that 78 of that comes from yield and 91 from rent growth but only 45 from renter affordability, and make an informed decision. That explainability is what separates this from a black box.

---

## 9. Infrastructure & DevOps

### 9.1 Local Development Stack

```
Docker:
  - Airflow (via Astro CLI — one command setup)
  - MinIO (local GCS emulation)

Non-Docker (runs in venv):
  - DuckDB (embedded, zero config)
  - dbt (CLI)
  - Evidence.dev (Node.js dev server)
  - MLflow (local SQLite backend)
```

### 9.2 Cloud Stack (Phase 5)

```
GCP:
  - GCS: gs://rental-opportunity-raw/     (Bronze file storage)
  - GCS: gs://rental-opportunity-delta/   (Delta table storage)
  - Cloud Run: evidence-dashboard          (public URL)
  - Cloud Scheduler: cron triggers

Snowflake (optional, free trial):
  - Database: RENTAL_OPPORTUNITY
    - Schema: BRONZE
    - Schema: SILVER
    - Schema: GOLD
```

### 9.3 Repo Structure

```
rental-opportunity-finder/
├── ingestion/              # API + CSV clients
├── dags/                   # Airflow DAGs
├── dbt/                    # dbt project
├── scoring/                # Opportunity score computation
├── dashboard/              # Evidence.dev pages
├── tests/                  # Python unit tests
├── config/                 # metros.yaml, zillow_files.yaml
├── data/                   # local DuckDB file (gitignored)
├── logs/                   # Airflow logs (gitignored)
├── infra/                  # docker-compose.yml
├── .env.example
├── .gitignore
├── Makefile
├── requirements.txt
└── README.md
```

---

## 10. Project Phases — Step-by-Step Build Plan

---

### PHASE 1 — Foundation (Week 1–2) ← IN PROGRESS
*Goal: Core infrastructure + first data source flowing end-to-end*

#### Step 1.1 — Repo & Environment Setup ✅
- [x] Create GitHub repo
- [x] Set up Python venv (pyenv + venv)
- [x] Create requirements.txt
- [x] Create .env.example
- [x] Write Makefile
- [x] Set up pre-commit hooks
- [x] Initialize dbt project
- [ ] **Commit:** initial scaffold

#### Step 1.2 — FRED API Ingest ← CURRENT STEP
- [x] Register for FRED API key
- [x] Write `ingestion/fred_client.py` with `fetch_series()` and `fetch_all_series()`
- [ ] Write unit tests: `tests/test_fred_client.py`
- [ ] Write `scripts/backfill_fred.py` to pull 5 years of history
- [ ] Save raw output to GCS/MinIO: `rental-opportunity-raw/fred/`
- [ ] Load to `bronze.fred_series_raw` Delta table
- [ ] **Commit:** FRED ingest client + tests

#### Step 1.3 — Local MinIO + DuckDB Delta Setup
- [ ] Add MinIO to docker-compose, start it
- [ ] Write `ingestion/storage_client.py` (boto3 wrapper for GCS/MinIO)
- [ ] Write `ingestion/bronze_loader.py` (writes Parquet to Delta via deltalake)
- [ ] Verify: `duckdb.sql("SELECT * FROM delta_scan('data/delta/bronze/fred_series_raw/')")`
- [ ] **Commit:** object storage + bronze delta working

#### Step 1.4 — First dbt Model (Silver FRED)
- [ ] Write `dbt/models/staging/stg_fred_series.sql`
- [ ] Write `dbt/models/silver/silver_mortgage_rates.sql` (pivot + forward-fill)
- [ ] Add schema.yml with `not_null`, `unique` tests
- [ ] `dbt run && dbt test` — all pass
- [ ] **Commit:** first dbt silver model

#### Step 1.5 — Airflow DAG for FRED
- [ ] Install Astro CLI, initialize Airflow project
- [ ] Write `dags/ingest_fred_weekly.py`
- [ ] Trigger manually, verify all tasks green
- [ ] **Commit:** Airflow + FRED DAG end-to-end

**Phase 1 Deliverable:** Automated FRED macro pipeline running in Airflow, writing to Bronze Delta, transforming to Silver via dbt.

---

### PHASE 2 — Zillow CSV Pipeline (Week 3–4)
*Goal: Rent and home value data flowing into Silver*

#### Step 2.1 — Zillow CSV Client
- [ ] Download ZORI and ZHVI CSVs manually first — inspect the schema
- [ ] Write `ingestion/zillow_csv_client.py`:
  - `download_zori_csv(output_path) → str`
  - `download_zhvi_csv(output_path) → str`
  - `melt_zori(df) → pd.DataFrame` (wide → long format)
  - `melt_zhvi(df) → pd.DataFrame`
- [ ] Write unit tests with fixture CSVs (small sample of the real file)
- [ ] **Commit:** Zillow CSV client + tests

#### Step 2.2 — Zillow Bronze Tables
- [ ] Write bronze schema for `zori_raw` and `zhvi_raw`
- [ ] Write bronze loader for Zillow (MERGE ON region_name + month)
- [ ] Run full backfill: load all historical data
- [ ] Verify row counts: ZORI should have ~2,500 zips × ~100 months = ~250k rows
- [ ] **Commit:** Zillow bronze tables loaded

#### Step 2.3 — Silver Rent and Home Value Models
- [ ] Write `dbt/models/staging/stg_zori.sql`
- [ ] Write `dbt/models/staging/stg_zhvi.sql`
- [ ] Write `dbt/models/silver/silver_rent_index.sql`
- [ ] Write `dbt/models/silver/silver_home_values.sql`
- [ ] Add HUD crosswalk as a dbt seed: `dbt/seeds/hud_zip_cbsa_crosswalk.csv`
- [ ] Join crosswalk to add `cbsa_code` to `silver_home_values`
- [ ] Add dbt tests: not_null, accepted ranges for rent/value
- [ ] `dbt run && dbt test` — all pass
- [ ] **Commit:** silver rent + home value models

#### Step 2.4 — Airflow Zillow DAG
- [ ] Write `dags/ingest_zillow_monthly.py`
- [ ] Test end-to-end with manual trigger
- [ ] **Commit:** Zillow DAG

**Phase 2 Deliverable:** Full rent + home value history in Silver. You can now query: "what is the median rent and home value for any US zip code?"

---

### PHASE 3 — Census + BLS + Gold Layer (Week 5–6)
*Goal: All data sources in Silver, Gold tables built, opportunity score computed*

#### Step 3.1 — Census API Client
- [ ] Write `ingestion/census_client.py`
- [ ] Bronze + Silver models for zip demographics
- [ ] **Commit**

#### Step 3.2 — BLS API Client
- [ ] Write `ingestion/bls_client.py`
- [ ] Bronze + Silver models for metro job growth
- [ ] **Commit**

#### Step 3.3 — Gold: Zip Investment Metrics
- [ ] Write `dbt/models/gold/gold_zip_investment_metrics.sql`
- [ ] Implement LAG-based YoY growth calculations
- [ ] Join all Silver tables
- [ ] Validate: check that yield, price-to-rent, rent growth all look reasonable
- [ ] **Commit**

#### Step 3.4 — Gold: Opportunity Score
- [ ] Write `scoring/score.py` (percentile ranking + composite score)
- [ ] Write unit tests: `tests/test_score.py`
  - Test that all scores land between 0 and 100
  - Test that tier thresholds are correct
  - Test that national_rank is sequential with no gaps
- [ ] Write `dbt/models/gold/gold_zip_opportunity_score.sql`
  (or keep as Python — either approach is valid)
- [ ] Add dbt tests on Gold output
- [ ] **Commit:** full Gold layer + scoring

#### Step 3.5 — Data Quality Layer
- [ ] Write dbt tests for all Gold tables:
  - `assert_yield_positive.sql`
  - `assert_score_between_0_100.sql`
  - `assert_price_to_rent_reasonable.sql` (between 5 and 50)
  - `assert_no_future_months.sql`
- [ ] Add `data_quality_checks` Airflow DAG
- [ ] **Commit:** data quality layer

**Phase 3 Deliverable:** `gold.zip_opportunity_score` populated with ranked scores for all US zips. You can query "SELECT * FROM gold.zip_opportunity_score ORDER BY national_rank LIMIT 20" and get a meaningful answer.

---

### PHASE 4 — Dashboard (Week 7–8)
*Goal: Live, usable dashboard showing the opportunity map*

#### Step 4.1 — Evidence.dev Setup
- [ ] `npm create evidence@latest` inside `dashboard/`
- [ ] Configure DuckDB connection
- [ ] Verify sample query runs
- [ ] **Commit:** Evidence scaffold

#### Step 4.2 — Build Page 1: National Opportunity Map
- [ ] Choropleth map colored by opportunity_score
- [ ] Top 50 table below the map
- [ ] Metric callout cards (national median rent, 30yr rate, etc.)
- [ ] **Commit**

#### Step 4.3 — Build Page 2: Zip Deep-Dive
- [ ] Score decomposition bar chart
- [ ] Rent trend + home value trend line charts
- [ ] Key stats table
- [ ] National rank + metro rank display
- [ ] **Commit**

#### Step 4.4 — Build Pages 3 + 4
- [ ] Metro comparison page
- [ ] Custom screener with filters
- [ ] **Commit:** all 4 dashboard pages

**Phase 4 Deliverable:** Fully functional local dashboard. You can open it and identify the top zip codes for rental investment with full score decomposition.

---

### PHASE 5 — Deployment (Week 9–10)
*Goal: Live public URL, cloud pipeline, portfolio-ready*

#### Step 5.1 — Cloud Migration
- [ ] Set up GCP project
- [ ] Migrate MinIO → GCS (change one env var)
- [ ] Migrate DuckDB → Snowflake for prod profile (change dbt target)
- [ ] Verify pipeline runs in cloud

#### Step 5.2 — Deploy Dashboard
- [ ] Deploy Evidence.dev to Cloud Run or Vercel
- [ ] Set up custom domain (optional)
- [ ] Verify public URL works

#### Step 5.3 — Cloud Scheduler
- [ ] Set up Cloud Scheduler to trigger Airflow DAGs on schedule
- [ ] Monitor first automated run

#### Step 5.4 — README + Documentation
- [ ] Write thorough README with architecture diagram
- [ ] Add screenshots of dashboard
- [ ] Add link to live demo
- [ ] Record 2-minute Loom walkthrough
- [ ] **Final commit**

---

### PHASE 6 — Stretch Goals (As Time Allows)

| Enhancement | Complexity | Value |
|---|---|---|
| HUD Fair Market Rents integration | Low | Second rent data source, cross-validation |
| Cap rate estimator (needs property tax data) | High | Most valuable investment metric |
| Email alert when zip score changes tier | Medium | Makes it a live tool, not just a dashboard |
| Terraform for GCP infra | Low | Resume polish |
| GitHub Actions CI/CD | Low | Resume polish |
| Redfin listing-level data | Medium | Enables property-level analysis |
| Historical score tracking | Low | Shows how scores change over time |

---

## 11. Tech Stack Summary

| Layer | Tool | Rationale |
|---|---|---|
| Orchestration | Apache Airflow (Astro CLI) | Industry standard, already in portfolio |
| Object Storage (local) | MinIO | S3-compatible, runs in Docker |
| Object Storage (cloud) | GCS | Already familiar |
| Table Format | Delta Lake (deltalake Python lib) | ACID transactions, no Spark needed |
| Transformations | dbt + DuckDB | Already set up in this project ✅ |
| Data Warehouse (dev) | DuckDB | Zero cost, zero config, fast |
| Data Warehouse (prod) | Snowflake | Production demo |
| Scoring | Python + pandas | Percentile ranking, explainable weights |
| Dashboard | Evidence.dev | Modern, markdown-based, strong portfolio signal |
| Data Quality | dbt tests | Shows production maturity |
| Cloud | GCP | Already familiar |

---

## 12. Risks & Mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Zillow changes CSV format or URL | Medium | Store raw files in GCS — only Silver breaks, not Bronze. Add schema validation on download. |
| ZORI coverage gaps (not all zips have rent data) | High | Expected — ZORI covers ~2,500 zips, ZHVI covers ~26,000. Scores only computed where both exist. Document coverage clearly. |
| Census ACS vintage lag (data is 1–2 years old) | Low | Expected and acceptable. Note vintage year on all demographic metrics. |
| BLS job data is metro-level, not zip-level | Medium | Accepted limitation. Document in README. All zips in a metro share the same job growth score. |
| DuckDB performance on full ZHVI history (~26k zips × 280 months) | Low | ~7M rows — DuckDB handles this easily. |
| Snowflake credit burn during deployment | Low | Use DuckDB for all dev. Snowflake only for final production demo. |

---

*Document Version: 2.0*
*Revised from: Housing Market Intelligence Platform v1.0*
*Key change: Refocused from general housing analytics to rental property investment opportunity scoring. Removed ML price estimator and listing-level Zillow API in favor of entirely free open data sources (Zillow Research CSVs, FRED, Census, BLS). Core pipeline architecture unchanged.*
*Author: Alex (UC Berkeley, MIDS/Haas)*
