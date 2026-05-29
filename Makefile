.PHONY: setup test dbt-run dbt-test format lint

setup:
	pip install -r requirements.txt

test:
	pytest tests/ -v

dbt-run:
	cd dbt && dbt run

dbt-test:
	cd dbt && dbt test

format:
	black .

lint:
	flake8 . --exclude .venv,__pycache__
