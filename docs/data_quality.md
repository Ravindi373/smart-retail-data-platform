# Data quality

Bad data is caught in Silver, moved to `quality.quarantine` (with the original
record as JSON and a reason), and never silently dropped. The rules live in
`src/quality/rules.py` (pure functions, unit-tested); the Silver DAG applies
them and records the outcome in `quality.run_summary`.

## Rule catalogue

| Check (from the brief) | failed_rule | Source | Layer where it is enforced |
|---|---|---|---|
| Primary keys are not null | `missing_transaction_id`, `missing_order_id`, `missing_product_id`, `missing_store_id`, `missing_po_id` | all | Silver |
| Required foreign key present | `missing_customer_id` | e-commerce, customers | Silver |
| Price and quantity are positive | `non_positive_quantity`, `negative_price`, `zero_price`, `invalid_quantity_format`, `invalid_price_format`, `invalid_order_item` | POS, e-commerce | Silver |
| Email format is valid before hashing | `invalid_email_format` | customers | Silver |
| Duplicate business keys are handled | `duplicate_business_key` | all | Silver |
| Timestamps parse | `unparseable_timestamp`, `invalid_snapshot_date` | POS, e-commerce, inventory | Silver |
| Nothing dated after its own extract | `future_dated_timestamp` | POS, e-commerce | Silver |
| Product identifier matches the master | `unknown_product_identifier` | POS, e-commerce, inventory | Silver |
| Missing / negative stock | `missing_quantity_on_hand`, `negative_quantity_on_hand` | inventory | Silver |
| Foreign keys match dimensions | dbt `relationships` tests | Gold facts | Gold (dbt) |
| Keys unique / not null | dbt `unique`, `not_null` | Gold | Gold (dbt) |
| Amounts positive, `net_sales = qty x price` | dbt singular tests | `fact_sales_transaction` | Gold (dbt) |
| Nothing vanishes | dbt test: clean + quarantined = rows in, and quarantine rows = rows_quarantined | all | Gold (dbt) |

When a check fails: the row is quarantined with its reason, processing of the
clean rows continues, and the counts appear in `gold.quality_scorecard` and
`gold.quality_summary` (the dashboard's data quality view).

## Results from the reference run (seed 42)

| Source | Rows in | Clean | Quarantined | Pass rate |
|---|---:|---:|---:|---:|
| pos_sales | 5,050 | 4,770 | 280 | 94.46% |
| ecommerce_orders | 3,000 | 2,912 | 88 | 97.07% |
| customers | 1,010 | 973 | 37 | 96.34% |
| inventory_snapshots | 505 | 493 | 12 | 97.62% |
| products | 300 | 300 | 0 | 100% |
| stores | 25 | 25 | 0 | 100% |
| supplier_deliveries | 300 | 300 | 0 | 100% |
| **Total** | **10,190** | **9,773** | **417** | |

| Source | Reason | Rows |
|---|---|---:|
| pos_sales | non_positive_quantity | 117 |
| pos_sales | negative_price | 92 |
| pos_sales | duplicate_business_key | 49 |
| pos_sales | unknown_product_identifier | 22 |
| ecommerce_orders | missing_customer_id | 54 |
| ecommerce_orders | future_dated_timestamp | 34 |
| customers | invalid_email_format | 27 |
| customers | duplicate_business_key | 10 |
| inventory_snapshots | missing_quantity_on_hand | 7 |
| inventory_snapshots | duplicate_business_key | 5 |

Every row is accounted for: clean + quarantined = rows in, for every source
(enforced by a dbt test and the end-to-end test).

## Known gap (reported, not hidden)

The 27 customers quarantined for an invalid email are absent from
`dim_customer`, so 338 sales lines (2.74% of net sales) reference a customer
that is not in the dimension. The dbt `relationships` test on
`fact_sales_transaction.customer_key` reports this as a **warning** (not an
error) and `sales_flat` labels these lines `unknown`. A production fix would keep
the customer with a masked email and flag the email problem separately.

## Why some checks are in Gold

Silver validates each table on its own. Cross-table referential integrity (does
this sale's customer exist?) can only be proven once the dimensions exist, so
dbt checks it in Gold.

## Privacy

Raw email and phone never leave Silver's input: `silver.customers` has only
`email_hash` and `phone_hash` (SHA-256 of the trimmed, lower-cased value with a
secret salt from `PII_HASH_SALT`). The end-to-end test fails if any raw email
address reaches Silver or Gold.
