# RPOF dashboard (Evidence 40.x)

Four pages over the opportunity scores: **National Overview**, **Zip Deep-Dive**
(full score decomposition), **Metro Comparison** and **Screener**.

```bash
make dashboard         # export the extract, then dev server on http://localhost:3000
make dashboard-build   # static site in dashboard/build (strict: fails on any query error)
```

Needs Node 18–22. On macOS: `brew install node@22` (keg-only; the Makefile finds it).

## Where the data comes from

Pages read `data/dashboard.duckdb`, a small read-only extract written by
`serving/dashboard_export.py` (Airflow refreshes it after every `compute_scores`
run). The dashboard never opens the warehouse itself:

- **Format:** Evidence 40.x bundles DuckDB 1.4, which can't read the DuckDB 2.0
  warehouse; the extract is written with `STORAGE_VERSION 'v1.4.0'`.
- **Locking:** DuckDB allows one writer or many readers per file, so a dashboard
  holding the warehouse open would block `dbt_build`.

## Versions

Pinned exactly (`package.json` + `package-lock.json`, installed with `npm ci`).
The project was scaffolded from the last open-source Evidence template
(`evidence-dev/template@4b13d42`, 2026-02-06). Evidence has since moved to the
account-based Evidence Studio; see design doc decision D12.

## Known vulnerabilities (read before deploying)

`npm audit` (2026-11-19) reports 57 advisories in the dependency tree (10 critical,
21 high), most with **no fix available**, because Evidence 40.x no longer gets
releases. They sit overwhelmingly in build and dev tooling (Storybook, the
TypeScript/ESLint parser, Vite and SvelteKit dev-server plugins).

- **Local use:** the dev server binds to `localhost` only (`make dashboard` does
  not pass `--host`); keep it that way. The build only processes this repo's files.
- **Before any public deployment (Phase 8):** re-run `npm audit`, review what ends
  up in the static bundle, or move off Evidence 40.x (the extract is tool-agnostic).
- Do not run `npm audit fix --force`: it breaks the pinned versions.
