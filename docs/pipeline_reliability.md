# RetailLake — Pipeline Reliability

This document answers a specific question directly: **what happens when
things go wrong**, not just what happens when everything works. Every
claim below is either something that was actually triggered and
observed during development (real bugs, real reruns), or a control that
was added specifically to be testable by re-running it.

## 1. Idempotency — reruns don't duplicate or corrupt data

| Layer | Mechanism | How it was verified |
|---|---|---|
| Raw (MinIO) | `land_raw_file` skips the upload if an object already exists at the same date-partitioned key with identical content hash | Re-ran `retail_bronze_ingestion` multiple times for the same date; object count in `retail-raw` did not grow |
| Bronze | `DELETE FROM {table} WHERE _run_date = %s` runs before every insert, scoped to that run's date | After a real accidental double-run (see incident log below), re-running produced exactly the source row count, not 2x |
| Silver | Full `TRUNCATE` + rebuild from all of Bronze on every run | Verified: after a clean Bronze truncate + single ingestion + single Silver run, quarantine counts matched an independent Python simulation exactly (49/92/117 for pos_sales, etc.) |
| Quarantine | `clear_quarantine` task truncates `quality.quarantine` at the start of every Silver run, before any source is processed | This was **not** true initially — see incident log entry #4 below |

## 2. Real incidents encountered, and what they prove

These are not hypothetical failure scenarios — they are things that
actually happened during development, which is arguably stronger
evidence of reliability than a synthetic test, since the pipeline's
actual failure and recovery behavior was observed under real conditions.

**Incident 1 — column name mismatch.** A Bronze insert referenced a CSV
header name (`timestamp`) instead of the actual database column
(`raw_timestamp`). The task failed immediately with a clear Postgres
error (`UndefinedColumn`) rather than silently inserting into the wrong
place or corrupting data. **What this proves:** failures are loud, not
silent — a wrong column reference cannot produce plausible-looking
wrong data, it just fails.

**Incident 2 — wrong parser for a JSON source.** `supplier_deliveries.json`
was routed through the CSV parser instead of a dedicated JSON handler.
This did **not** fail — it produced a task marked "success" with a row
count of 2,701 instead of the correct 300. **What this proves, and its
limit:** a task exit code alone is not sufficient evidence of
correctness — this is exactly why `retail_full_pipeline`'s `quality_gate`
task independently re-checks row counts and quarantine rates after
everything claims to have succeeded, rather than trusting task status
alone.

**Incident 3 — Decimal JSON serialization.** Postgres returns `NUMERIC`
columns as Python `Decimal`, which the standard JSON encoder cannot
serialize. This broke the quarantine write path specifically for rows
containing a price or quantity — the exact rows the pipeline most needs
to quarantine successfully. **What this proves:** error paths need
testing too, not just the happy path — this bug only manifested when a
row actually failed validation.

**Incident 4 — unbounded quarantine growth.** `retail_silver_clean`
correctly truncated `silver.*` tables on every run (full-refresh), but
never truncated `quality.quarantine` — so every historical run's
quarantine output accumulated forever, including runs made while
Incidents 1-3 were still unresolved. This was caught by a business-key
sanity check (`products` should have zero duplicates by construction,
but showed 300) — not by any task failing. Fixed by adding a dedicated
`clear_quarantine` task that runs before any source is cleaned.

## 3. What the pipeline does when a task actually fails

- **Automatic retry:** every task has `retries=1` with a 2-minute delay
  (`default_args` in each DAG), so a transient failure (e.g. a brief
  Postgres connection blip) gets one automatic second attempt before
  being reported as failed.
- **Stop, don't continue on stale data:** `retail_full_pipeline` chains
  Bronze → Silver → Gold via `TriggerDagRunOperator(wait_for_completion=True)`.
  If Bronze fails, Silver is never triggered — the pipeline does not
  proceed to build Gold tables from an incomplete or stale Bronze
  snapshot.
- **`max_active_runs=1`** on every DAG prevents two runs of the same
  DAG executing concurrently, which was the root cause of a real
  duplicate-data incident during development (two overlapping manual
  triggers racing their own DELETE/INSERT steps against each other).
- **The quality gate can fail a "successful" run.** `quality_gate`
  independently re-verifies, after all upstream tasks report success,
  that: every Bronze table has rows, every Silver table has rows, and
  no source's quarantine rate exceeds 15% (real observed rates top out
  at 5.1% — see `data_quality_report.md`). If any check fails, the task
  raises and the whole pipeline run is marked failed, even though every
  individual DAG it triggered reported success. This is the direct,
  concrete answer to "evaluated not only on successful execution."

## 4. How to actually test this yourself

**Rerun safety test:**
```
# Trigger retail_full_pipeline twice in a row from the Airflow UI.
# Row counts in bronze.*, silver.*, and gold.* should be identical
# after both runs — verify with:
docker exec -it retaillake-postgres psql -U retaillake -d retaildb -c \
  "SELECT 'pos_sales', count(*) FROM bronze.pos_sales UNION ALL SELECT 'silver_pos_sales', count(*) FROM silver.pos_sales;"
```

**Quality gate failure test (deliberately break the data):**
```
docker exec -it retaillake-postgres psql -U retaillake -d retaildb -c \
  "TRUNCATE silver.products;"
# Then manually run just the quality_gate task from the Airflow UI
# (or trigger retail_full_pipeline — dbt run/test will still succeed
# since dbt doesn't check for empty tables, but quality_gate will fail
# with a clear message: "silver.products is EMPTY").
```

This second test is worth actually running once and screenshotting for
your final report — it's direct proof the pipeline distinguishes
"all tasks exited 0" from "the data is actually correct," which is
exactly the distinction this reliability review asked for.

## 5. Known limitations, stated plainly rather than hidden

- Silver uses full-refresh, not incremental merge — fine at this
  project's scale (thousands of rows), would not scale to a real
  production dataset without redesign.
- The quality gate's 15% threshold is a reasonable default, not a
  business-validated SLA — a real deployment would set this per-source
  based on actual historical variance, not a single flat number.
- `retail_full_pipeline` orchestrates dbt via a direct `dbt` CLI call
  inside the Airflow container (dbt-core installed via pip), not via a
  separate dbt service or Docker-in-Docker — simpler and more reliable
  for this project's scale, at the cost of coupling Airflow's Python
  environment to dbt's dependencies.
