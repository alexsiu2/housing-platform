.PHONY: help setup up down restart ps logs test lint format dbt-build dbt-debug compose-check \
	backfill-fred backfill-zillow load-hud backfill-bls load-census load-nri backfill-gdelt up-streaming score export-dashboard dashboard dashboard-build up-spark backfill-hmda smoke-test run-dag port-check

# Compose reads ${VAR} substitutions (AIRFLOW_PORT, AIRFLOW_UID, ...) from the
# compose file's own directory (docker/.env) by default; use the root .env.
ENV_FILE := $(if $(wildcard .env),--env-file .env)
COMPOSE := docker compose -f docker/compose.yaml $(ENV_FILE) --profile core

# Use the project virtualenv when it exists, so every target (python, pytest,
# dbt, black, ...) works without activating it first. CI has no .venv and uses
# the tools it installed.
VENV := $(CURDIR)/.venv
ifneq ($(wildcard $(VENV)/bin/python),)
export PATH := $(VENV)/bin:$(PATH)
PYTHON := $(VENV)/bin/python
else
PYTHON := $(shell command -v python3 || command -v python)
endif
# Tools run as `$(PYTHON) -m ...`: macOS ships GNU Make 3.81, which ignores a
# changed PATH for commands it runs without a shell, so PATH alone isn't enough.
DBT := cd dbt && dbt
DBT_FLAGS := --profiles-dir .

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

setup:  ## Create .venv if missing, install Python dev dependencies + pre-commit hooks
	test -x .venv/bin/python || python3 -m venv .venv
	.venv/bin/pip install -r requirements-dev.txt
	.venv/bin/pre-commit install

up: port-check  ## Start the core local stack (SeaweedFS + Airflow)
	mkdir -p logs/airflow data
	$(COMPOSE) up -d --build

port-check:  ## Fail early, naming the culprit, if the Airflow port (AIRFLOW_PORT) is taken
	@scripts/check_port.sh

down:  ## Stop the local stack, including Spark/streaming if running (keeps data volumes)
	$(COMPOSE) --profile spark --profile streaming down

up-streaming: port-check  ## Start the core stack + Kafka, Schema Registry, Kafka UI (:8091) and the news producers
	mkdir -p logs/airflow data
	$(COMPOSE) --profile streaming up -d --build

up-spark: port-check  ## Start the core stack + Spark master/worker (needs ~10 GB Docker memory)
	mkdir -p logs/airflow data
	$(COMPOSE) --profile spark up -d --build

# HMDA year Y is published in spring Y+1, so the newest loadable year is last year.
HMDA_LAST_YEAR := $(shell expr $(shell date +%Y) - 1)
HMDA_YEARS ?= $(shell seq 2018 $(HMDA_LAST_YEAR))

backfill-hmda:  ## Run spark_hmda in Airflow for 2018..last year, e.g. make backfill-hmda YEARS="2024 2025"
	scripts/run_dag.sh spark_hmda 14400 '{"years": [$(shell echo $(or $(YEARS),$(HMDA_YEARS)) | tr ' ' ',')]}'

restart: down up  ## Restart the local stack

ps:  ## Show stack status
	$(COMPOSE) ps

logs:  ## Tail stack logs
	$(COMPOSE) logs -f --tail=100

test:  ## Run unit tests
	$(PYTHON) -m pytest tests/ -v

lint:  ## Check formatting and lint
	$(PYTHON) -m black --check .
	$(PYTHON) -m flake8 .

format:  ## Auto-format Python code
	$(PYTHON) -m black .

dbt-build:  ## Run and test all dbt models against local DuckDB
	mkdir -p data
	$(PYTHON) -m ingestion.warehouse_lock run -- sh -c 'cd dbt && dbt build $(DBT_FLAGS)'

dbt-debug:  ## Check the dbt connection
	$(DBT) debug $(DBT_FLAGS)

compose-check:  ## Validate the compose file without starting anything
	$(COMPOSE) config -q

backfill-fred:  ## Backfill FRED to Bronze, e.g. make backfill-fred START=2019-01-01 [END=...]
	$(PYTHON) scripts/backfill_fred.py $(if $(START),--start $(START)) $(if $(END),--end $(END))

backfill-zillow:  ## Backfill full Zillow ZORI + ZHVI history to Bronze
	$(PYTHON) scripts/backfill_zillow.py

load-hud:  ## Load HUD zip-cbsa + tract-zip crosswalks (needs HUD_API_TOKEN), e.g. YEAR=2025 QUARTER=2
	$(PYTHON) scripts/load_hud_crosswalk.py $(if $(YEAR),--year $(YEAR)) $(if $(QUARTER),--quarter $(QUARTER))

backfill-bls:  ## Backfill BLS CES metro employment (needs HUD crosswalk in Bronze), e.g. START=2014
	$(PYTHON) scripts/backfill_bls.py $(if $(START),--start $(START))

load-census:  ## Load ACS 5-year vintages by ZCTA, e.g. make load-census VINTAGES="2019 2020 2021 2022 2023 2024"
	$(PYTHON) scripts/load_census.py $(if $(VINTAGES),--vintages $(VINTAGES))

load-nri:  ## Load FEMA National Risk Index (hazard) tracts into Bronze
	$(PYTHON) scripts/load_fema_nri.py

backfill-gdelt:  ## Backfill GDELT housing news via BigQuery, e.g. START=2024-01-01 [END=...]
	$(PYTHON) scripts/backfill_gdelt.py --start $(or $(START),2024-01-01) $(if $(END),--end $(END))

score:  ## Compute gold.zip_opportunity_score from the latest Gold month
	$(PYTHON) -m ingestion.warehouse_lock run -- $(PYTHON) -m scoring.score

export-dashboard:  ## Refresh data/dashboard.duckdb (the dashboard's extract) from the warehouse
	$(PYTHON) -m ingestion.warehouse_lock run -- $(PYTHON) -m serving.dashboard_export

# Evidence 40.x needs Node 18-22. Homebrew's node@22 is keg-only, so use it if present.
NODE_BIN := $(firstword $(wildcard /opt/homebrew/opt/node@22/bin /usr/local/opt/node@22/bin))
NPM := $(if $(NODE_BIN),PATH=$(NODE_BIN):$$PATH npm,npm)

dashboard: export-dashboard  ## Run the dashboard dev server on http://localhost:3000
	cd dashboard && { test -d node_modules || $(NPM) ci; } && $(NPM) run sources && $(NPM) run dev -- --port 3000

dashboard-build: export-dashboard  ## Build the static dashboard site into dashboard/build
	cd dashboard && $(NPM) ci && $(NPM) run sources:strict && $(NPM) run build:strict

smoke-test:  ## Trigger platform_smoke_test in the running stack and wait for success
	scripts/run_dag.sh platform_smoke_test

run-dag:  ## Trigger a DAG and wait, e.g. make run-dag DAG=ingest_fred
	scripts/run_dag.sh $(DAG)
