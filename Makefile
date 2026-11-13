.PHONY: help setup up down restart ps logs test lint format dbt-build dbt-debug compose-check \
	backfill-fred backfill-zillow load-hud backfill-bls load-census score smoke-test run-dag port-check

# Compose reads ${VAR} substitutions (AIRFLOW_PORT, AIRFLOW_UID, ...) from the
# compose file's own directory (docker/.env) by default; use the root .env.
ENV_FILE := $(if $(wildcard .env),--env-file .env)
COMPOSE := docker compose -f docker/compose.yaml $(ENV_FILE) --profile core
DBT := cd dbt && dbt
DBT_FLAGS := --profiles-dir .

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

setup:  ## Install Python dev dependencies and pre-commit hooks
	pip install -r requirements-dev.txt
	pre-commit install

up: port-check  ## Start the core local stack (SeaweedFS + Airflow)
	mkdir -p logs/airflow data
	$(COMPOSE) up -d --build

port-check:  ## Fail early, naming the culprit, if the Airflow port (AIRFLOW_PORT) is taken
	@scripts/check_port.sh

down:  ## Stop the local stack (keeps data volumes)
	$(COMPOSE) down

restart: down up  ## Restart the local stack

ps:  ## Show stack status
	$(COMPOSE) ps

logs:  ## Tail stack logs
	$(COMPOSE) logs -f --tail=100

test:  ## Run unit tests
	pytest tests/ -v

lint:  ## Check formatting and lint
	black --check .
	flake8 .

format:  ## Auto-format Python code
	black .

dbt-build:  ## Run and test all dbt models against local DuckDB
	mkdir -p data
	$(DBT) build $(DBT_FLAGS)

dbt-debug:  ## Check the dbt connection
	$(DBT) debug $(DBT_FLAGS)

compose-check:  ## Validate the compose file without starting anything
	$(COMPOSE) config -q

backfill-fred:  ## Backfill FRED to Bronze, e.g. make backfill-fred START=2019-01-01 [END=...]
	python scripts/backfill_fred.py $(if $(START),--start $(START)) $(if $(END),--end $(END))

backfill-zillow:  ## Backfill full Zillow ZORI + ZHVI history to Bronze
	python scripts/backfill_zillow.py

load-hud:  ## Load HUD zip-cbsa + tract-zip crosswalks (needs HUD_API_TOKEN), e.g. YEAR=2025 QUARTER=2
	python scripts/load_hud_crosswalk.py $(if $(YEAR),--year $(YEAR)) $(if $(QUARTER),--quarter $(QUARTER))

backfill-bls:  ## Backfill BLS CES metro employment (needs HUD crosswalk in Bronze), e.g. START=2014
	python scripts/backfill_bls.py $(if $(START),--start $(START))

load-census:  ## Load ACS 5-year vintages by ZCTA, e.g. make load-census VINTAGES="2019 2020 2021 2022 2023 2024"
	python scripts/load_census.py $(if $(VINTAGES),--vintages $(VINTAGES))

score:  ## Compute gold.zip_opportunity_score from the latest Gold month
	python -m scoring.score

smoke-test:  ## Trigger platform_smoke_test in the running stack and wait for success
	scripts/run_dag.sh platform_smoke_test

run-dag:  ## Trigger a DAG and wait, e.g. make run-dag DAG=ingest_fred
	scripts/run_dag.sh $(DAG)
