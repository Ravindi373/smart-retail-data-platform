# Source systems

Five mock retail source systems plus two reference files, generated locally (no
real customer data) by `scripts/seed_mock_sources.py` with a fixed random seed.
Output is byte-identical on every run and every operating system.

| # | Source | Format | Rows | Key fields |
|---|---|---|---|---|
| 1 | POS sales | CSV | 5,050 | transaction_id, store_id, product_id, customer_id, quantity, unit_price, timestamp, payment_method |
| 2 | E-commerce orders | JSON | 3,000 | order_id, customer_id, items[] (product_id or sku, quantity, unit_price), order_timestamp, channel, status |
| 3 | Warehouse stock | CSV | 505 | snapshot_date, product_id, warehouse_id, quantity_on_hand, reorder_point |
| 4 | CRM customers | CSV | 1,010 | customer_id, name, email, phone, loyalty_tier, created_at |
| 5 | Supplier deliveries | JSON | 300 | po_id, supplier_id, product_id, ordered_qty, delivered_qty, ordered_date, delivered_date |
| ref | Products | CSV | 300 | product_id, sku, name, category, unit_cost, unit_price |
| ref | Stores | CSV | 25 | store_id, region (Western/Central/Southern/Northern), channel |

`data/sample/_manifest.json` records the seed, the **extract timestamp**
(2026-08-23T12:00:00) and the row count of every file. The extract timestamp is
the reference point for detecting future-dated records.

## Deliberately injected data problems

| Required issue | Where it is injected | How the pipeline handles it |
|---|---|---|
| Missing values | e-commerce `customer_id`; inventory `quantity_on_hand` | quarantined: `missing_customer_id`, `missing_quantity_on_hand` |
| Duplicate records | POS transaction ids, customers, inventory snapshots | first copy kept, the rest quarantined: `duplicate_business_key` |
| Invalid prices / quantities | POS negative or zero quantity, negative price | `non_positive_quantity`, `negative_price` (zero price -> `zero_price`) |
| Mixed timestamp formats | POS and e-commerce: `YYYY-MM-DDTHH:MM:SS`, `MM/DD/YYYY HH:MM`, `DD-MM-YYYY` | parsed to UTC; unparseable -> `unparseable_timestamp` |
| Different product identifiers | POS rows and e-commerce items send SKUs (some lower-case / padded); some POS rows carry an unknown SKU | resolved to the product master; unknown -> `unknown_product_identifier` |
| Future-dated orders | e-commerce order timestamps after the extract time | `future_dated_timestamp` |
| Invalid email format | CRM `email` contains `_at_` instead of `@` | `invalid_email_format` (checked before hashing) |

The required minimums from the brief are all exceeded (POS 5,000+, e-commerce
3,000+, customers 1,000+, products 300+, inventory 500+); a unit test enforces them.

## Regenerating

```bash
pip install -r scripts/requirements.txt
python scripts/seed_mock_sources.py --seed 42 --out-dir data/sample
```

A unit test fails if the committed `data/sample` differs from what the generator
produces, so the files in Git can always be reproduced.
