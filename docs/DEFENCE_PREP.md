# Defence preparation

You are assessed on explaining your own work (15% ownership + 10% demonstration,
and evidence throughout). The brief says copied or unexplained code will not
count. Read the file named next to each answer until you can explain it without
looking. Answers are short on purpose - put them in your own words.

## Architecture

**Why medallion layers?** Each layer has one job, so a problem can be traced to one place: Raw keeps the original, Bronze types and audits, Silver cleans, Gold models. If a cleaning rule is wrong we reprocess from Raw. (`docs/architecture.md`)

**Why must the dashboard read only Gold?** Gold is the tested, trusted layer at the grain a dashboard needs. Reading Silver or Raw would bypass the tests and quarantine.

**Why is dbt not inside Airflow?** dbt and the pinned Airflow image have conflicting dependencies, so dbt has its own container and runs via `make gold`. Trade-off: Gold is not triggered by the DAG.

**Why Postgres for quarantine?** Rejected rows are queryable with SQL and appear on the dashboard. It is not an immutable audit store; that is a stated limitation.

## Ingestion and re-runs

**How do you prevent duplicates on a re-run?** Raw: same content at the same key is skipped. Bronze: rows for that `_run_date` are deleted then re-inserted.

**What if the source file changes on the same date?** Raw is never overwritten: the new content lands beside the original as `<name>.<hash>.<ext>`.

**What happens tomorrow when `@daily` runs again?** Bronze gets a second load. Silver reads only the latest load per source, so nothing is counted twice.

**Why does Silver rebuild everything each run?** Full refresh is simple and always consistent at this size. Production would merge incrementally.

## Data quality

**How do you define "future-dated"?** After the source extract time (`_manifest.json`), not after today's date, so the result is the same whenever the pipeline runs.

**How are different product identifiers handled?** POS and e-commerce send SKUs (sometimes lower-case or padded). Silver looks each one up against the product master (id or SKU, normalised) and stores the canonical `product_id`. Unknown ones are quarantined, not guessed.

**Why hash emails, and is SHA-256 enough?** Analysts don't need raw PII. A plain hash of a common email can be reversed with a lookup table, so a secret salt (`PII_HASH_SALT`) is added. Raw and Bronze still hold raw values by design.

**Why validate the email before hashing?** Hashing a malformed address would create a "valid-looking" key from garbage. Invalid ones are quarantined first.

**Explain the dbt WARN.** Some customers were quarantined for an invalid email, so their sales lines point at a customer missing from `dim_customer`. Silver validates each table alone; only Gold can check cross-table keys. A production fix: keep the customer with a masked email and flag the email problem separately.

**How do you know no rows vanish?** For every source clean + quarantined = rows in. Enforced by a dbt test and the end-to-end test.

## Modelling

**State the grain of each fact table.** `fact_sales_transaction`: one row per sales line item. `fact_inventory_snapshot`: one row per product, per location, per day.

**Why is `store_key` NULL for online sales?** An online order has no physical store. The relationships test only checks non-null values.

**How is `is_stockout_risk` defined?** `quantity_on_hand <= reorder_point`.

## Engineering

**What do your tests prove?** Unit tests on the rules, storage and generator; DAG structure tests; and an end-to-end test that runs the real DAGs and dbt on Postgres.

**What did you find and fix in Weeks 7-8?** See `docs/CHANGES_WEEK7_8.md` - future-dated orders, re-run duplication, region data, raw overwrite, a committed secret, and more.

**What was your hardest bug?** Pick a real one - the merge conflict resolution, the port binding issues, or something from `docs/TROUBLESHOOTING.md` - and be ready to explain cause and fix.

**What would you do with more time?** Incremental Silver, dbt triggered from Airflow, an unknown-customer member in `dim_customer`, encrypt/restrict Raw and Bronze PII, alerting on quality thresholds.

## AI usage (be honest and specific)

Be ready to say which parts you drafted with AI help, and to walk through any
file line by line. If you cannot explain a piece of code, re-read it now, run it,
and change something small to see what breaks.
