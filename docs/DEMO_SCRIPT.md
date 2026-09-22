# Demo script (about 12 minutes)

Goal: prove the flow **source -> Raw -> Bronze -> Silver -> Gold -> dashboard**
works, that bad data is handled visibly, and that you can explain every step.
Say the *why*, not just the *what*. All values below come from seed 42, so they
are the same every time.

## Before the session (10 minutes earlier)

1. `make up`, wait for all containers to be healthy (`docker compose ps`).
2. Do one full `make pipeline` so you know it works on this machine today.
3. Open, in separate tabs: Airflow (8080), MinIO console (9001), Superset dashboard (8088), the GitHub repo, the latest green GitHub Actions run, and a terminal in the repo folder.
4. Have `psql` ready: `docker exec -it retaillake-postgres psql -U <user> -d retaildb`.
5. Keep the `evidence/` screenshots open as a fallback if anything is slow.

## Run of show

| Min | Show | Say |
|---|---|---|
| 0-1 | README architecture diagram | "Retail teams have POS, online, warehouse, CRM and supplier data in different places and bad rows reach their reports. I built a local platform that lands everything, cleans it, quarantines bad rows, models a star schema and shows a dashboard." Name the three users. |
| 1-2 | Layer table in README | "Raw is never changed. Bronze is typed and audited. Silver is the only place cleaning happens. Gold is what the dashboard reads - nothing else." |
| 2-4 | `data/sample/` and `_manifest.json` | "Generated data, fixed seed, so anyone gets identical files. It contains every problem the brief lists." Show one bad row each: a negative quantity in `pos_sales.csv`, a `SKU...` product id, an `_at_` email. |
| 4-7 | Airflow: trigger **`retail_pipeline`** | "One entry point: Bronze, wait, Silver, same run date." While it runs, open MinIO: `retail-raw/pos_sales/YYYY/MM/DD/`. Then run `SELECT source_name, file_path, row_count, run_date FROM bronze.ingestion_audit_log ORDER BY audit_id DESC LIMIT 7;` - "every load records source, rows and time." |
| 7-8 | **Re-run the same date** | Trigger Bronze again for the same date, show row counts unchanged and the log line "skipped - identical content already landed". "Re-running never duplicates data, and raw is never overwritten." |
| 8-9 | Silver log: the quality report | Open the `log_quality_report` task log. "Pass rate per source and the reasons rows were rejected. Nothing vanishes: clean plus quarantined equals rows in." |
| 9-10 | Bronze vs Silver | Show a POS row's `product_id` in Bronze (a SKU) vs Silver (resolved to `PROD-...`). "Different systems, different identifiers; Silver resolves them to the master." Then show a quarantined future-dated order and its original JSON. Then `SELECT customer_id, left(email_hash,16) FROM silver.customers LIMIT 2;` - "only hashes past Bronze." |
| 10-11 | `make gold` | "10 models, 38 tests: all pass except one warning, and I can explain it: customers with an invalid email were quarantined, so their sales lines have no customer row. I report it instead of hiding it." State the grain of each fact table. |
| 11-13 | Superset dashboard | Answer the manager question: "Can you understand sales, stock and data health in three minutes?" Apply a filter (channel or region) and show charts change together. Click a chart -> its dataset -> `sales_flat` -> `fact_sales_transaction`. |
| 13-14 | GitHub Actions + one test | Show the green run. Open `tests/unit/test_rules.py`, show a validation test. "The pipeline is tested end to end in CI." |

Leave 5 minutes for questions. Use `docs/DEFENCE_PREP.md` to prepare.

## Cold-start version

`make demo` runs: start stack, generate data, unpause DAGs, Bronze, Silver, Gold,
quality summary. Use it only if you have time; otherwise start warm.

## If something breaks

| Problem | Do this |
|---|---|
| Airflow slow or UI down | run `make bronze` and `make silver` in the terminal and show the same logs |
| `retail_pipeline` waits forever | children are paused: `make unpause` |
| Superset slow | show the screenshots in `evidence/` and explain which Gold table feeds each chart |
| Anything else | `docs/TROUBLESHOOTING.md`; say what you would check first. Handling a failure calmly is part of the assessment. |
