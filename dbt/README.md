# dbt project

Staging, Silver and Gold models for the Rental Property Opportunity Finder.
Bronze tables are written by `ingestion/` and read here as sources.

```bash
make dbt-build   # from the repo root
```

Models write to the local DuckDB file at `data/rpof.duckdb` (override with `DUCKDB_PATH`).
