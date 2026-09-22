# Weeks 7-8: what changed and why

Week 7 is "test, monitor and polish"; Week 8 is "final submission". While
preparing them, the pipeline was checked against the project brief and several
gaps were found and fixed. This page lists every change so you can explain it.

## Problems found and fixed

| # | Problem | How it showed | Fix | Where |
|---|---|---|---|---|
| 1 | **Future-dated orders reached the dashboard** | Daily Sales Trend dropped to almost zero at the right edge; 34 orders dated after the extract | new `future_dated_timestamp` rule; "future" is measured against the source extract time | `src/quality/rules.py`, `retail_silver_clean.py` |
| 2 | **A second daily run would double Silver** | Bronze keeps a copy per `_run_date`; Silver read all of it, so the second day's copies would be quarantined as duplicates | Silver reads only the latest Bronze load per source | `retail_silver_clean.py` (`_latest`) |
| 3 | **"Different product identifiers" missing** | the brief requires it; the generator never varied identifiers and Silver had no mapping | generator sends SKUs (some padded/lower-case, some unknown); Silver resolves via the product master; unknown -> quarantine | `seed_mock_sources.py`, `rules.py` |
| 4 | **`dim_store.region` always NULL** | the region filter could not work | new `stores.csv` source -> Bronze -> Silver -> `dim_store` (4 regions, `not_null` test) | generator, both DAGs, `dim_store.sql` |
| 5 | **Raw could be overwritten** | changed content for the same date replaced the object, against "do not overwrite" | changed content lands beside the original under a hash-suffixed key | `src/connectors/storage.py` |
| 6 | **Real-looking secret in `.env.example`** | `SUPERSET_SECRET_KEY` was committed | placeholder + a CI check that fails on secret-like values | `.env.example`, `ci.yml` |
| 7 | **Fresh clone: Superset had no database** | init SQL never created `superset` | added `CREATE DATABASE superset` | `docker/postgres-init/01-init-databases.sql` |
| 8 | **Weak PII hashing** | plain SHA-256 of an email can be reversed with a lookup table; empty values hashed to one shared hash | optional secret salt (`PII_HASH_SALT`); empty -> NULL | `rules.py`, `docker-compose.yml` |
| 9 | Week 4 items not implemented | "trim and normalise text fields" | `clean_text` / `clean_label` applied in Silver | `rules.py`, `retail_silver_clean.py` |
| 10 | CSV line endings differed between Windows and Linux | regenerated files differed byte-for-byte | generator writes LF; `.gitattributes` | generator |

## Added

| What | Why |
|---|---|
| `dags/retail_pipeline.py` master DAG | one scheduled entry point: Bronze, wait, Silver (children now unscheduled) |
| `quality.run_summary` + `gold.quality_scorecard` | pass rate per source, not just failure counts |
| `_log_quality_report` task | quality summary counts in the Airflow log |
| structured logging in both DAGs, rollback on failure | Week 7 "improve logging and error messages" |
| dbt: 4 singular tests, loyalty `accepted_values`, `region not_null` | more Gold checks |
| `tests/` (unit, DAG, end-to-end) | Week 7 tests |
| `.github/workflows/ci.yml` | Week 7 GitHub Actions: lint, unit, e2e, secrets check |
| `Makefile` | `make demo`, `make pipeline`, `make quality`, `make test` ... |
| `README.md`, `docs/*` | Week 7 README gaps, troubleshooting; Week 8 demo script and defence prep |

## Numbers that changed

| | Before | After |
|---|---:|---:|
| `fact_sales_transaction` rows | 12,115 | 12,014 |
| Quarantined POS rows | 258 | 280 (+22 unknown product) |
| Quarantined e-commerce rows | 54 | 88 (+34 future-dated) |
| Customer-FK warning rows | 339 | 338 |

Earlier screenshots show the *before* numbers. Recapture them.

## Your checklist after these changes

1. **Rotate the Superset key.** The old value is in Git history. Generate a new one (`python -c "import secrets; print(secrets.token_hex(32))"`) and put it in your local `.env`.
2. **Add `PII_HASH_SALT`** to your `.env` (generate the same way).
3. **Create the `superset` database** if your Postgres volume already exists: `docker exec -it retaillake-postgres psql -U <user> -c "CREATE DATABASE superset;"`.
4. `make up` (or `docker compose ... up -d --force-recreate`), then `make seed` (regenerates `stores.csv`, `_manifest.json` and the updated POS/e-commerce files; commit them).
5. `make unpause`, then `make pipeline`, then `make quality`. Expect: POS 4,770 clean / 280 quarantined; e-commerce 2,912 / 88; dbt `PASS=47 WARN=1 ERROR=0`.
6. **Superset:** for datasets `sales_flat`, `inventory_flat`, `quality_summary` use *Edit -> Sync columns from source*; add `quality_scorecard` as a dataset and a pass-rate table chart; point the **Store / region** filter at `region`; check the loyalty-tier filter now shows `none` and `unknown`.
7. **Recapture screenshots** into `evidence/final/`.
8. Fill the unpinned versions in the README "Installed versions" table.
9. Push, watch the **Actions** run go green (fix anything it reports), then rehearse `docs/DEMO_SCRIPT.md`.
10. Replace the name / student-ID placeholders in the report, re-export the PDF, name it `CCA_DataEngineer_[Name]_[ID]_FinalSubmission`.
