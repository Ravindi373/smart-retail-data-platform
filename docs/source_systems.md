# Source systems

Five mock retail source systems, generated locally (no real customer data)
by `scripts/seed_mock_sources.py` with a fixed random seed for repeatability.

## 1. POS sales
- **Format:** CSV
- **Purpose:** In-store transactions, one row per sale line item
- **Volume:** 5,000+ rows
- **Fields (planned):** transaction_id, store_id, product_id, customer_id,
  quantity, unit_price, timestamp, payment_method
- **Known issues to inject:** duplicate transaction_id, negative quantity or
  price, mixed timestamp formats

## 2. E-commerce orders
- **Format:** JSON
- **Purpose:** Online orders and order events
- **Volume:** 3,000+ rows
- **Fields (planned):** order_id, customer_id, items[] (product_id, qty,
  price), order_timestamp, channel, status
- **Known issues to inject:** future-dated orders, missing customer_id,
  mixed timestamp formats

## 3. Warehouse stock
- **Format:** PostgreSQL table or CSV
- **Purpose:** Daily inventory snapshots by product and location
- **Volume:** 500+ rows
- **Fields (planned):** snapshot_date, product_id, warehouse_id,
  quantity_on_hand, reorder_point
- **Known issues to inject:** missing values, duplicate snapshot rows

## 4. CRM customers
- **Format:** CSV or mock API
- **Purpose:** Customer profile records
- **Volume:** 1,000+ rows
- **Fields (planned):** customer_id, name, email, phone, loyalty_tier,
  created_at
- **Known issues to inject:** invalid email format, missing customer_id,
  duplicate customer_id

## 5. Supplier deliveries
- **Format:** JSON or Parquet
- **Purpose:** Purchase order / delivery performance
- **Volume:** not specified in brief — target 300+ rows to match product
  catalogue scale
- **Fields (planned):** po_id, supplier_id, product_id, ordered_qty,
  delivered_qty, ordered_date, delivered_date

## Products (reference/dimension source)
- **Format:** CSV
- **Volume:** 300+ rows
- **Fields (planned):** product_id, sku, name, category, unit_cost,
  unit_price

## Required bad-data patterns (across all sources)
- Missing values (e.g. null customer_id)
- Duplicate records (e.g. duplicate transaction_id)
- Invalid prices or quantities (negative or zero where not allowed)
- Mixed timestamp formats (e.g. ISO 8601 vs `MM/DD/YYYY`)
- Different product identifiers across systems (POS SKU vs CRM product code
  vs supplier part number — needs a mapping step in Silver)

## Open questions for mentor sync
- Should supplier delivery volume follow the 300-row product catalogue or
  get its own minimum?
- Is a mock REST API required for CRM, or is CSV acceptable for Week 1–2?
