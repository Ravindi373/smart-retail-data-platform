# Gold layer ERD

Star schema: two fact tables, four dimensions, built as dbt models in
`dbt_smart_retail/models/gold/`.

```mermaid
erDiagram
  dim_customer ||--o{ fact_sales_transaction : "purchases"
  dim_product ||--o{ fact_sales_transaction : "sold_in"
  dim_store ||--o{ fact_sales_transaction : "sold_at"
  dim_date ||--o{ fact_sales_transaction : "occurs_on"
  dim_product ||--o{ fact_inventory_snapshot : "stocked_as"
  dim_store ||--o{ fact_inventory_snapshot : "held_at"
  dim_date ||--o{ fact_inventory_snapshot : "snapshot_on"

  dim_customer {
    string customer_key PK
    string name
    string email_hash
    string phone_hash
    string loyalty_tier
    date created_at
  }
  dim_product {
    string product_key PK
    string sku
    string product_name
    string category
    numeric unit_cost
    numeric unit_price
  }
  dim_store {
    string store_key PK
    string store_id
    string region
    string channel
  }
  dim_date {
    date date_key PK
    int year
    int month
    int day
    int quarter
    string day_of_week
    boolean is_weekend
  }
  fact_sales_transaction {
    string transaction_line_id PK
    string customer_key FK
    string product_key FK
    string store_key FK
    date date_key FK
    int quantity
    numeric unit_price
    numeric net_sales
    string channel
  }
  fact_inventory_snapshot {
    string snapshot_id PK
    string product_key FK
    string store_key FK
    date date_key FK
    int quantity_on_hand
    int reorder_point
    boolean is_stockout_risk
  }
```

## Grain

- `fact_sales_transaction`: one row per sales line item (one product within one POS transaction or online order)
- `fact_inventory_snapshot`: one row per product, per store/warehouse, per snapshot date

## Decisions

- `channel` is kept on the fact table (in_store / online / mobile_app) and on `dim_store` (always `in_store`): an online order has no physical store, so `store_key` is legitimately NULL for online sales.
- PII (`email_hash`, `phone_hash`) is hashed in Silver and never stored raw in Gold.
- `is_stockout_risk` is derived: `quantity_on_hand <= reorder_point`.
- `net_sales` = `quantity x unit_price` (checked by a dbt test).
- Customers without a loyalty membership are labelled `none` in `dim_customer`. A sale whose customer is missing from the dimension is labelled `unknown` in `sales_flat` (see Known limitations in the README).

## Extra Gold tables (not part of the star)

`quality_summary` (quarantine counts per source and rule), `quality_scorecard` (pass rate per source), and the dashboard views `sales_flat` / `inventory_flat`.
