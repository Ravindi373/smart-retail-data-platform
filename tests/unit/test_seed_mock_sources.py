"""Tests for scripts/seed_mock_sources.py: the brief requires repeatable
generated data, minimum volumes, and specific injected bad-data patterns."""
import csv
import json
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pytest

from quality.rules import is_valid_email, parse_mixed_timestamp

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "seed_mock_sources.py"
FILES = ["customers.csv", "ecommerce_orders.json", "inventory_snapshots.csv", "pos_sales.csv",
         "products.csv", "stores.csv", "supplier_deliveries.json", "_manifest.json"]


def generate(out_dir, seed=42):
    subprocess.run([sys.executable, str(SCRIPT), "--seed", str(seed), "--out-dir", str(out_dir)],
                   check=True, capture_output=True)
    return Path(out_dir)


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


@pytest.fixture(scope="module")
def data(tmp_path_factory):
    return generate(tmp_path_factory.mktemp("seed42"))


# ------------------------------------------------------------ repeatability
def test_same_seed_gives_byte_identical_files(data, tmp_path):
    again = generate(tmp_path / "again")
    for name in FILES:
        assert (data / name).read_bytes() == (again / name).read_bytes(), name


def test_different_seed_gives_different_data(data, tmp_path):
    other = generate(tmp_path / "other", seed=7)
    assert (data / "pos_sales.csv").read_bytes() != (other / "pos_sales.csv").read_bytes()


def test_committed_sample_data_matches_the_generator(data):
    """data/sample in the repo must be exactly what the generator produces, so
    a reviewer who regenerates gets the same files (and the same numbers)."""
    committed = SCRIPT.parents[1] / "data" / "sample"
    for name in FILES:
        assert (committed / name).read_bytes() == (data / name).read_bytes(), (
            f"{name} is stale - run: python scripts/seed_mock_sources.py")


def test_csv_files_use_unix_line_endings(data):
    assert b"\r" not in (data / "pos_sales.csv").read_bytes()


# ---------------------------------------------------------- minimum volumes
def test_minimum_row_counts_from_the_brief(data):
    assert len(read_csv(data / "pos_sales.csv")) >= 5000
    assert len(json.loads((data / "ecommerce_orders.json").read_text())) >= 3000
    assert len(read_csv(data / "customers.csv")) >= 1000
    assert len(read_csv(data / "products.csv")) >= 300
    assert len(read_csv(data / "inventory_snapshots.csv")) >= 500


def test_manifest_matches_the_files(data):
    manifest = json.loads((data / "_manifest.json").read_text())
    counts = manifest["row_counts"]
    assert counts["pos_sales"] == len(read_csv(data / "pos_sales.csv"))
    assert counts["customers"] == len(read_csv(data / "customers.csv"))
    assert counts["stores"] == len(read_csv(data / "stores.csv"))
    assert counts["ecommerce_orders"] == len(json.loads((data / "ecommerce_orders.json").read_text()))
    assert manifest["seed"] == 42 and "T" in manifest["extract_timestamp"]


# ------------------------------------------------- required bad-data patterns
def test_pos_contains_duplicate_transaction_ids(data):
    ids = Counter(r["transaction_id"] for r in read_csv(data / "pos_sales.csv"))
    assert any(n > 1 for n in ids.values())


def test_pos_contains_negative_quantity_and_negative_price(data):
    rows = read_csv(data / "pos_sales.csv")
    assert any(float(r["quantity"]) < 0 for r in rows)
    assert any(float(r["unit_price"]) < 0 for r in rows)


def test_pos_contains_mixed_timestamp_formats(data):
    rows = read_csv(data / "pos_sales.csv")
    shapes = {("T" in r["timestamp"], "/" in r["timestamp"]) for r in rows}
    assert len(shapes) >= 3          # ISO, slash-US, dash-day-first


def test_customers_contain_invalid_emails(data):
    rows = read_csv(data / "customers.csv")
    assert any(not is_valid_email(r["email"]) for r in rows)


def test_orders_contain_missing_customer_ids_and_future_dates(data):
    orders = json.loads((data / "ecommerce_orders.json").read_text())
    assert any(not o["customer_id"] for o in orders)
    extract = datetime.fromisoformat(
        json.loads((data / "_manifest.json").read_text())["extract_timestamp"]
    ).replace(tzinfo=timezone.utc)
    assert any(parse_mixed_timestamp(o["order_timestamp"]) > extract for o in orders)


def test_inventory_contains_missing_quantities_and_duplicates(data):
    rows = read_csv(data / "inventory_snapshots.csv")
    assert any(r["quantity_on_hand"] == "" for r in rows)
    keys = Counter((r["snapshot_date"], r["product_id"], r["warehouse_id"]) for r in rows)
    assert any(n > 1 for n in keys.values())


def test_sources_use_different_product_identifiers(data):
    products = read_csv(data / "products.csv")
    skus = {p["sku"] for p in products}
    ids = {p["product_id"] for p in products}
    pos_ids = {r["product_id"] for r in read_csv(data / "pos_sales.csv")}
    orders = json.loads((data / "ecommerce_orders.json").read_text())
    item_keys = {k for o in orders for item in o["items"] for k in item}

    assert {i.strip().upper() for i in pos_ids} & skus       # POS sends SKUs
    assert "SKU999999" in pos_ids                             # ...and some unknown ones
    assert "sku" in item_keys and "product_id" in item_keys   # e-commerce mixes both
    assert not (skus & ids)                                   # the two namespaces really differ


def test_store_master_has_regions(data):
    stores = read_csv(data / "stores.csv")
    assert len(stores) == 25
    assert {s["region"] for s in stores} == {"Western", "Central", "Southern", "Northern"}
