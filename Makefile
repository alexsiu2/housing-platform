.PHONY: help setup up down restart ps logs test lint format dbt-build dbt-debug compose-check

COMPOSE := docker compose -f docker/compose.yaml --profile core
DBT := cd dbt && dbt
DBT_FLAGS := --profiles-dir .

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

setup:  ## Install Python dev dependencies and pre-commit hooks
	pip install -r requirements-dev.txt
	pre-commit install

up:  ## Start the core local stack (SeaweedFS + Airflow)
	mkdir -p logs/airflow data
	$(COMPOSE) up -d --build

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
