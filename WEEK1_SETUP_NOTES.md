# RetailLake — Smart Retail Data Platform

Eight-week individual data engineering project (CCA Data Engineer internship).
Turns messy, scattered retail data (POS, e-commerce, warehouse, CRM, suppliers)
into clean, trusted, dashboard-ready tables using a medallion architecture.

## Status
Week 1 — planning and architecture. Not yet runnable.

## Problem
Retail teams can't make good decisions because POS, online, warehouse and CRM
data live in separate systems, reports are stitched together by hand in
spreadsheets, and bad records flow straight into dashboards with no review
step. See `docs/source_systems.md` for the five source systems this project
ingests and `docs/architecture.md` for the full data flow.

## Architecture (high level)

Sources -> Raw (MinIO) -> Bronze (typed) -> Silver (clean) -> Gold (star schema) -> Superset dashboard

No layer skips ahead: the dashboard only ever queries Gold. Raw is never
edited after landing. See `docs/architecture.md` for the full diagram and
the rules for each layer.

## Planned stack

| Area | Tool |
|---|---|
| Language | Python 3.11+ |
| Orchestration | Apache Airflow (Astro CLI) |
| Object storage | MinIO |
| Database | PostgreSQL 16 / DuckDB |
| Transforms | dbt (Bronze/Silver/Gold), Python for cleaning |
| Quality | Great Expectations + dbt tests |
| Dashboard | Apache Superset |
| Environment | Docker Compose |
| CI | GitHub Actions |

## Repository structure

```
smart-retail-data-platform/
├── .github/workflows/        # CI: lint, tests (Week 7)
├── dags/                     # Airflow DAGs (Week 3+)
├── dbt_smart_retail/models/
│   ├── bronze/                # typed source tables
│   ├── silver/                # cleaned, deduplicated, PII-hashed
│   └── gold/                  # facts + dimensions for BI
├── src/connectors/            # source-system loaders
├── src/quality/                # validation / quarantine logic
├── scripts/                    # seed_mock_sources.py etc.
├── tests/                      # unit tests
├── docker/docker-compose.yml   # local stack (Week 2)
├── docs/                       # architecture, ERD, source docs
├── .env.example
└── README.md
```

## Planned setup (Week 2 — not yet built)

1. `cp .env.example .env` and fill in local values
2. `docker compose -f docker/docker-compose.yml up -d`
3. `python scripts/seed_mock_sources.py` to generate sample retail data
4. Trigger Airflow DAGs to land raw files
5. `dbt build` to run Bronze/Silver/Gold models
6. Open Superset and view the dashboard

## Weekly plan

| Week | Focus |
|---|---|
| 1 | Plan: problem, sources, architecture, ERD |
| 2 | Setup: Docker, MinIO, DB, seed data |
| 3 | Ingest: raw landing + Bronze parsing |
| 4 | Clean: Silver layer + first quality checks |
| 5 | Model: Gold star schema + dbt tests |
| 6 | Dashboards: Superset charts + quality view |
| 7 | Reliability: CI, tests, logging, fixes |
| 8 | Handover: report, presentation, demo |

## Individual ownership

This is an individual project. All code, decisions and explanations are my
own. AI tools may assist drafting and learning, but every design choice
here needs to be defensible in the final demo.
