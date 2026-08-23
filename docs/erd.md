# Gold layer ERD (draft — Week 1)

Star schema: two fact tables, four dimensions. This will be built as dbt
models in `dbt_smart_retail/models/gold/` from Week 5 onward. Grain and
keys below are a first draft and may change once Silver cleaning reveals
real data shapes.

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
    string customer_id_hashed
    string loyalty_tier
    string email_hash
  }
  dim_product {
    string product_key PK
    string sku
    string product_name
    string category
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
    string day_of_week
  }
  fact_sales_transaction {
    string transaction_line_id PK
    string customer_key FK
    string product_key FK
    string store_key FK
    date date_key FK
    int quantity
    decimal unit_price
    decimal net_sales
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

- `fact_sales_transaction`: one row per sales line item (one product within
  one order/transaction)
- `fact_inventory_snapshot`: one row per product, per store/warehouse, per
  snapshot date

## Notes / decisions to confirm with mentor

- `channel` lives on both `dim_store` and the fact table temporarily —
  decide in Week 5 whether channel is a store attribute or its own
  dimension (a store can sell both in-person and online).
- PII (`email_hash`, `customer_id_hashed`) is hashed in Silver, never
  stored raw in Gold.
- `is_stockout_risk` is a derived/calculated column, not a raw source
  field — logic to be defined against `reorder_point` in Week 5.
