"""Unit tests for src/quality/rules.py - the validation and cleaning helpers
that decide which rows are clean and which are quarantined."""
from datetime import datetime, timezone

import pytest

from quality.rules import (
    build_product_lookup,
    clean_label,
    clean_text,
    hash_pii,
    is_future_dated,
    is_valid_email,
    normalise_key,
    parse_mixed_timestamp,
    resolve_as_of,
    resolve_order_items,
    resolve_product_id,
    validate_customer,
    validate_ecommerce_order,
    validate_inventory_snapshot,
    validate_pos_sale,
)

UTC = timezone.utc
AS_OF = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
PRODUCTS = [
    {"product_id": "PROD-00001", "sku": "SKU000001"},
    {"product_id": "PROD-00002", "sku": "SKU000002"},
]
LOOKUP = build_product_lookup(PRODUCTS)


# ---------------------------------------------------------------- timestamps
@pytest.mark.parametrize("raw, expected", [
    ("2026-03-05T14:30:00", datetime(2026, 3, 5, 14, 30, tzinfo=UTC)),   # ISO
    ("03/05/2026 14:30", datetime(2026, 3, 5, 14, 30, tzinfo=UTC)),       # US style
    ("05-03-2026", datetime(2026, 3, 5, 0, 0, tzinfo=UTC)),               # day-first
    ("  2026-03-05T14:30:00  ", datetime(2026, 3, 5, 14, 30, tzinfo=UTC)), # padded
])
def test_parse_mixed_timestamp_known_formats(raw, expected):
    assert parse_mixed_timestamp(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "   ", "yesterday", "2026/03/05", "31-31-2026"])
def test_parse_mixed_timestamp_rejects_unknown(raw):
    assert parse_mixed_timestamp(raw) is None


def test_parsed_timestamps_are_utc_aware():
    assert parse_mixed_timestamp("2026-03-05T14:30:00").utcoffset().total_seconds() == 0


# ------------------------------------------------------------- text cleaning
def test_clean_text_trims_and_collapses_whitespace():
    assert clean_text("  Acme   Widget \n Pro ") == "Acme Widget Pro"
    assert clean_text("   ") is None and clean_text(None) is None


def test_clean_label_lowercases():
    assert clean_label("  ONLINE ") == "online"
    assert clean_label(None) is None


def test_normalise_key_makes_identifiers_comparable():
    assert normalise_key(" sku000001 ") == "SKU000001"


# --------------------------------------------------------------------- email
@pytest.mark.parametrize("email, ok", [
    ("jane@example.com", True), ("a.b+c@sub.example.org", True), (" jane@example.com ", True),
    ("jane_at_example.com", False), ("jane@example", False), ("@example.com", False),
    ("jane doe@example.com", False), ("", False), (None, False),
])
def test_is_valid_email(email, ok):
    assert is_valid_email(email) is ok


# ------------------------------------------------------------------- hashing
def test_hash_pii_is_deterministic_and_format_insensitive():
    assert hash_pii("Jane@Example.com", salt="") == hash_pii("  jane@example.com ", salt="")


def test_hash_pii_is_64_hex_and_hides_the_value():
    digest = hash_pii("jane@example.com", salt="")
    assert len(digest) == 64 and all(c in "0123456789abcdef" for c in digest)
    assert "jane" not in digest and "@" not in digest


def test_hash_pii_salt_changes_the_hash():
    assert hash_pii("jane@example.com", salt="s1") != hash_pii("jane@example.com", salt="s2")
    assert hash_pii("jane@example.com", salt="s1") != hash_pii("jane@example.com", salt="")


def test_hash_pii_reads_salt_from_environment(monkeypatch):
    monkeypatch.setenv("PII_HASH_SALT", "from-env")
    assert hash_pii("x@y.zz") == hash_pii("x@y.zz", salt="from-env")


@pytest.mark.parametrize("empty", [None, "", "   "])
def test_hash_pii_of_nothing_is_none_not_a_shared_hash(empty):
    assert hash_pii(empty) is None


# ------------------------------------------------------------- future dating
def test_is_future_dated():
    assert is_future_dated(datetime(2026, 8, 24, tzinfo=UTC), AS_OF)
    assert not is_future_dated(datetime(2026, 8, 23, 12, 0, tzinfo=UTC), AS_OF)  # equal is fine
    assert not is_future_dated(datetime(2026, 1, 1, tzinfo=UTC), AS_OF)
    assert not is_future_dated(None, AS_OF)


def test_resolve_as_of_precedence(tmp_path):
    manifest = tmp_path / "_manifest.json"
    manifest.write_text('{"extract_timestamp": "2026-08-23T12:00:00"}')
    # 1. explicit env override wins
    dt, origin = resolve_as_of(str(manifest), env={"SOURCE_EXTRACT_TS": "2026-01-01T00:00:00"})
    assert dt == datetime(2026, 1, 1, tzinfo=UTC) and origin == "env:SOURCE_EXTRACT_TS"
    # 2. then the manifest
    dt, origin = resolve_as_of(str(manifest), env={})
    assert dt == AS_OF and origin == "manifest"
    # 3. then the fallback (injected clock keeps the test deterministic)
    now = datetime(2030, 1, 1, tzinfo=UTC)
    dt, origin = resolve_as_of(str(tmp_path / "missing.json"), env={}, now=now)
    assert dt == now and origin == "fallback:now"


# ------------------------------------------------------ product identifiers
@pytest.mark.parametrize("raw", ["PROD-00001", "prod-00001", "SKU000001", " sku000001 "])
def test_resolve_product_id_accepts_id_or_sku_in_any_case(raw):
    assert resolve_product_id(raw, LOOKUP) == "PROD-00001"


@pytest.mark.parametrize("raw", ["SKU999999", "", None, "PROD-9"])
def test_resolve_product_id_unknown_is_none(raw):
    assert resolve_product_id(raw, LOOKUP) is None


def test_resolve_order_items_maps_skus_to_canonical_ids():
    items = [{"product_id": "PROD-00001", "quantity": 1, "unit_price": 9.5},
             {"sku": "sku000002 ", "quantity": 2, "unit_price": 3.0}]
    resolved, reason = resolve_order_items(items, LOOKUP)
    assert reason is None
    assert [i["product_id"] for i in resolved] == ["PROD-00001", "PROD-00002"]
    assert resolved[1]["quantity"] == 2 and resolved[1]["unit_price"] == 3.0


@pytest.mark.parametrize("items, reason", [
    ([{"sku": "SKU999999", "quantity": 1, "unit_price": 5}], "unknown_product_identifier"),
    ([{"product_id": "PROD-00001", "quantity": 0, "unit_price": 5}], "invalid_order_item"),
    ([{"product_id": "PROD-00001", "quantity": 1, "unit_price": -5}], "invalid_order_item"),
    ([{"product_id": "PROD-00001", "quantity": "x", "unit_price": 5}], "invalid_order_item"),
    ([], "invalid_order_item"), (None, "invalid_order_item"), (["nope"], "invalid_order_item"),
])
def test_resolve_order_items_rejects_the_whole_order(items, reason):
    resolved, got = resolve_order_items(items, LOOKUP)
    assert resolved is None and got == reason


# ---------------------------------------------------------------- POS sales
def pos(**overrides):
    row = {"transaction_id": "TXN-1", "store_id": "STORE-001", "product_id": "PROD-00001",
           "customer_id": "CUST-1", "quantity": 2, "unit_price": 9.99,
           "timestamp": "2026-08-01T10:00:00", "payment_method": "card"}
    row.update(overrides)
    return row


def test_valid_pos_sale_passes_all_checks():
    assert validate_pos_sale(pos(), as_of=AS_OF, product_lookup=LOOKUP) is None


@pytest.mark.parametrize("overrides, reason", [
    ({"quantity": -1}, "non_positive_quantity"),
    ({"quantity": 0}, "non_positive_quantity"),
    ({"quantity": "abc"}, "invalid_quantity_format"),
    ({"quantity": None}, "invalid_quantity_format"),
    ({"unit_price": -5.0}, "negative_price"),
    ({"unit_price": 0}, "zero_price"),
    ({"unit_price": "free"}, "invalid_price_format"),
    ({"transaction_id": ""}, "missing_transaction_id"),
    ({"transaction_id": None}, "missing_transaction_id"),
    ({"timestamp": "not a date"}, "unparseable_timestamp"),
    ({"timestamp": "2026-08-24T00:00:00"}, "future_dated_timestamp"),
    ({"product_id": "SKU999999"}, "unknown_product_identifier"),
])
def test_invalid_pos_sale_reasons(overrides, reason):
    assert validate_pos_sale(pos(**overrides), as_of=AS_OF, product_lookup=LOOKUP) == reason


def test_pos_sale_with_sku_instead_of_product_id_is_valid():
    assert validate_pos_sale(pos(product_id=" sku000001 "), as_of=AS_OF, product_lookup=LOOKUP) is None


def test_pos_checks_are_optional_so_the_function_stays_usable_alone():
    assert validate_pos_sale(pos(timestamp="2999-01-01T00:00:00", product_id="???")) is None


# ---------------------------------------------------------------- customers
def test_customer_rules():
    ok = {"customer_id": "CUST-1", "email": "jane@example.com"}
    assert validate_customer(ok) is None
    assert validate_customer({**ok, "customer_id": ""}) == "missing_customer_id"
    assert validate_customer({**ok, "email": "jane_at_example.com"}) == "invalid_email_format"
    assert validate_customer({**ok, "email": None}) == "invalid_email_format"


# ------------------------------------------------------------ e-commerce
def order(**overrides):
    row = {"order_id": "ORD-1", "customer_id": "CUST-1", "order_timestamp": "03/05/2026 14:30",
           "items": [{"product_id": "PROD-00001", "quantity": 1, "unit_price": 4.0}]}
    row.update(overrides)
    return row


def test_valid_order_passes():
    assert validate_ecommerce_order(order(), as_of=AS_OF, product_lookup=LOOKUP) is None


@pytest.mark.parametrize("overrides, reason", [
    ({"order_id": None}, "missing_order_id"),
    ({"customer_id": None}, "missing_customer_id"),
    ({"order_timestamp": "garbage"}, "unparseable_timestamp"),
    ({"order_timestamp": "2026-09-15T09:00:00"}, "future_dated_timestamp"),
    ({"items": [{"sku": "SKU999999", "quantity": 1, "unit_price": 4}]}, "unknown_product_identifier"),
    ({"items": []}, "invalid_order_item"),
])
def test_invalid_order_reasons(overrides, reason):
    assert validate_ecommerce_order(order(**overrides), as_of=AS_OF, product_lookup=LOOKUP) == reason


# ----------------------------------------------------------------- inventory
def snap(**overrides):
    row = {"snapshot_date": "2026-08-01", "product_id": "PROD-00001", "quantity_on_hand": 10}
    row.update(overrides)
    return row


def test_inventory_rules():
    assert validate_inventory_snapshot(snap(), product_lookup=LOOKUP) is None
    assert validate_inventory_snapshot(snap(quantity_on_hand=0)) is None           # zero stock is real
    assert validate_inventory_snapshot(snap(quantity_on_hand=None)) == "missing_quantity_on_hand"
    assert validate_inventory_snapshot(snap(quantity_on_hand="")) == "missing_quantity_on_hand"
    assert validate_inventory_snapshot(snap(quantity_on_hand=-3)) == "negative_quantity_on_hand"
    assert validate_inventory_snapshot(snap(quantity_on_hand="lots")) == "invalid_quantity_format"
    assert validate_inventory_snapshot(snap(snapshot_date="01/08/2026")) == "invalid_snapshot_date"
    assert validate_inventory_snapshot(snap(product_id="SKU999999"), product_lookup=LOOKUP) == \
        "unknown_product_identifier"
