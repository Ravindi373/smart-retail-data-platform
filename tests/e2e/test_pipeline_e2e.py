"""End-to-end test of the whole platform: generated sources -> Raw (S3) ->
Bronze -> Silver (+ quarantine) -> Gold (dbt) -> dashboard datasets.

It runs the REAL Airflow DAGs (`airflow dags test`) and the REAL dbt project,
so it proves the pieces connect - the brief's definition of "complete".

Skipped unless RETAIL_E2E=1. It needs, all configured through the same
environment variables the containers use (see .env.example):
  * Postgres      POSTGRES_HOST / _USER / _PASSWORD / _DB
  * S3 endpoint   MINIO_ENDPOINT / MINIO_ROOT_USER / MINIO_ROOT_PASSWORD
  * Airflow       AIRFLOW_HOME + AIRFLOW__CORE__DAGS_FOLDER (CLI: $AIRFLOW_BIN)
  * dbt 1.8       $DBT_BIN, run with --profiles-dir docker/dbt-profiles
Locally: `make e2e`. In CI: the `e2e` job in .github/workflows/ci.yml.
"""
import json
import os
import subprocess
from pathlib import Path

import boto3
import psycopg2
import pytest

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(os.environ.get("RETAIL_E2E") != "1", reason="set RETAIL_E2E=1 to run"),
]

ROOT = Path(__file__).resolve().parents[2]
SAMPLE = Path(os.environ.get("RETAIL_SAMPLE_DIR", ROOT / "data" / "sample"))
AIRFLOW = os.environ.get("AIRFLOW_BIN", "airflow")
DBT = os.environ.get("DBT_BIN", "dbt")
DAY1, DAY2 = "2026-09-21", "2026-09-22"
SOURCES = ["pos_sales", "ecommerce_orders", "customers", "products", "stores",
           "inventory_snapshots", "supplier_deliveries"]


# ------------------------------------------------------------------ helpers
def connect():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "retaildb"),
        user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
    )


def scalar(sql, *args):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchone()[0]


def rows(sql, *args):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def run(cmd, cwd=ROOT):
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    assert result.returncode == 0, f"{' '.join(cmd)} failed:\n{result.stdout[-3000:]}\n{result.stderr[-3000:]}"
    return result.stdout + result.stderr


def dag_test(dag_id, day):
    return run([AIRFLOW, "dags", "test", dag_id, day])


def dbt_build():
    return run([DBT, "build", "--profiles-dir", str(ROOT / "docker" / "dbt-profiles"), "--no-use-colors"],
               cwd=ROOT / "dbt_smart_retail")


def raw_object_count():
    s3 = boto3.client(
        "s3", endpoint_url=os.environ["MINIO_ENDPOINT"], region_name="us-east-1",
        aws_access_key_id=os.environ["MINIO_ROOT_USER"], aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"])
    return s3, len(s3.list_objects_v2(Bucket="retail-raw").get("Contents", []))


@pytest.fixture(scope="module")
def manifest():
    return json.loads((SAMPLE / "_manifest.json").read_text())


@pytest.fixture(scope="module")
def pipeline(manifest):
    """Start from an empty platform, then run every layer once."""
    with connect() as conn, conn.cursor() as cur:
        for schema in ("bronze", "silver", "gold", "quality"):
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    s3 = boto3.client(
        "s3", endpoint_url=os.environ["MINIO_ENDPOINT"], region_name="us-east-1",
        aws_access_key_id=os.environ["MINIO_ROOT_USER"], aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"])
    try:
        s3.create_bucket(Bucket="retail-raw")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    for obj in s3.list_objects_v2(Bucket="retail-raw").get("Contents", []):
        s3.delete_object(Bucket="retail-raw", Key=obj["Key"])

    run([AIRFLOW, "db", "migrate"])
    out = {"bronze": dag_test("retail_bronze_ingestion", DAY1),
           "silver": dag_test("retail_silver_clean", DAY1),
           "dbt": dbt_build()}
    out["silver_counts"] = rows("SELECT source_name, rows_clean, rows_quarantined FROM quality.run_summary ORDER BY 1")
    out["quarantine_total"] = scalar("SELECT count(*) FROM quality.quarantine")
    return out


# ------------------------------------------------------------- Raw + Bronze
def test_raw_files_landed_in_date_folders(pipeline):
    s3, count = raw_object_count()
    assert count == len(SOURCES)
    keys = {o["Key"] for o in s3.list_objects_v2(Bucket="retail-raw")["Contents"]}
    for source in SOURCES:
        assert any(k.startswith(f"{source}/2026/09/21/") for k in keys), source


@pytest.mark.parametrize("source", SOURCES)
def test_bronze_row_count_matches_the_source_file(pipeline, manifest, source):
    assert scalar(f"SELECT count(*) FROM bronze.{source}") == manifest["row_counts"][source]


def test_bronze_carries_ingestion_metadata(pipeline):
    assert scalar("SELECT count(*) FROM bronze.pos_sales WHERE _source_file IS NULL "
                  "OR _ingested_at IS NULL OR _run_date IS NULL") == 0
    audit = dict(rows("SELECT source_name, row_count FROM bronze.ingestion_audit_log"))
    assert set(audit) == set(SOURCES)


# ----------------------------------------------------------------- Silver
def test_every_source_row_is_clean_or_quarantined(pipeline, manifest):
    for name, clean, quarantined in pipeline["silver_counts"]:
        assert clean + quarantined == manifest["row_counts"][name], name
    assert pipeline["quarantine_total"] == sum(q for _, _, q in pipeline["silver_counts"])


def test_all_the_required_bad_data_types_are_quarantined(pipeline):
    found = {r[0] for r in rows("SELECT DISTINCT failed_rule FROM quality.quarantine")}
    assert {"non_positive_quantity", "negative_price", "duplicate_business_key",
            "invalid_email_format", "missing_customer_id", "future_dated_timestamp",
            "unknown_product_identifier", "missing_quantity_on_hand"} <= found


def test_quarantine_keeps_the_original_record(pipeline):
    sample = rows("SELECT record_json, failed_rule FROM quality.quarantine LIMIT 50")
    assert sample and all(isinstance(rec, dict) and rec for rec, _ in sample)


def test_no_future_dated_records_reach_silver(pipeline, manifest):
    extract = manifest["extract_timestamp"]
    assert scalar("SELECT count(*) FROM silver.pos_sales WHERE txn_timestamp > %s::timestamptz", extract) == 0
    assert scalar("SELECT count(*) FROM silver.ecommerce_orders WHERE order_timestamp > %s::timestamptz", extract) == 0


def test_every_silver_product_reference_resolves_to_the_master(pipeline):
    assert scalar("SELECT count(*) FROM silver.pos_sales s LEFT JOIN silver.products p USING (product_id) "
                  "WHERE p.product_id IS NULL") == 0
    assert scalar("SELECT count(*) FROM silver.inventory_snapshots s LEFT JOIN silver.products p USING (product_id) "
                  "WHERE p.product_id IS NULL") == 0
    assert scalar("SELECT count(*) FROM silver.ecommerce_orders o, jsonb_array_elements(o.items) i "
                  "LEFT JOIN silver.products p ON p.product_id = i->>'product_id' WHERE p.product_id IS NULL") == 0


def test_pii_is_hashed_and_never_stored_raw(pipeline):
    assert scalar("SELECT count(*) FROM silver.customers WHERE email_hash !~ '^[0-9a-f]{64}$'") == 0
    assert scalar("SELECT count(*) FROM silver.customers WHERE email_hash IS NULL OR phone_hash IS NULL") == 0
    columns = {r[0] for r in rows("SELECT column_name FROM information_schema.columns "
                                  "WHERE table_schema = 'silver' AND table_name = 'customers'")}
    assert "email" not in columns and "phone" not in columns
    # no column of the Gold customer dimension may contain an email address
    assert scalar("SELECT count(*) FROM gold.dim_customer WHERE strpos(dim_customer::text, '@') > 0") == 0


# ------------------------------------------------------------------- Gold
def test_dbt_build_finished_without_errors(pipeline):
    assert "ERROR=0" in pipeline["dbt"]
    assert "PASS=" in pipeline["dbt"]


def test_gold_sales_fact_reconciles_with_silver(pipeline):
    online_lines = scalar("SELECT COALESCE(sum(jsonb_array_length(items)), 0) FROM silver.ecommerce_orders")
    assert scalar("SELECT count(*) FROM gold.fact_sales_transaction") == \
        scalar("SELECT count(*) FROM silver.pos_sales") + online_lines


def test_gold_inventory_fact_matches_silver(pipeline):
    assert scalar("SELECT count(*) FROM gold.fact_inventory_snapshot") == \
        scalar("SELECT count(*) FROM silver.inventory_snapshots")


def test_store_dimension_has_real_regions(pipeline):
    regions = {r[0] for r in rows("SELECT DISTINCT region FROM gold.dim_store")}
    assert regions == {"Western", "Central", "Southern", "Northern"}


def test_dashboard_datasets_carry_every_required_filter(pipeline):
    cols = {r[0] for r in rows("SELECT column_name FROM information_schema.columns "
                               "WHERE table_schema = 'gold' AND table_name = 'sales_flat'")}
    assert {"date_key", "channel", "region", "store_id", "category", "loyalty_tier", "net_sales"} <= cols


def test_dashboard_has_no_sales_after_the_extract_date(pipeline, manifest):
    assert scalar("SELECT count(*) FROM gold.sales_flat WHERE date_key > %s::date",
                  manifest["extract_timestamp"][:10]) == 0


def test_quality_scorecard_covers_every_source(pipeline):
    assert {r[0] for r in rows("SELECT source_name FROM gold.quality_scorecard")} == set(SOURCES)


# ------------------------------------------------ idempotency and re-runs
# (kept last: these re-run DAGs, so they must not affect the tests above)
def test_rerunning_the_same_date_duplicates_nothing(pipeline):
    _, raw_before = raw_object_count()
    bronze_before = scalar("SELECT count(*) FROM bronze.pos_sales")
    dag_test("retail_bronze_ingestion", DAY1)
    _, raw_after = raw_object_count()
    assert raw_after == raw_before
    assert scalar("SELECT count(*) FROM bronze.pos_sales") == bronze_before


def test_the_next_days_run_does_not_inflate_silver_or_quarantine(pipeline):
    """@daily reloads the same extract under a new _run_date. Silver must still
    describe ONE copy of the data, not two."""
    dag_test("retail_bronze_ingestion", DAY2)
    assert scalar("SELECT count(DISTINCT _run_date) FROM bronze.pos_sales") == 2
    dag_test("retail_silver_clean", DAY2)
    assert rows("SELECT source_name, rows_clean, rows_quarantined FROM quality.run_summary ORDER BY 1") == \
        pipeline["silver_counts"]
    assert scalar("SELECT count(*) FROM quality.quarantine") == pipeline["quarantine_total"]
