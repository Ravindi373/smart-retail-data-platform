"""
src/quality/rules.py

Pure, dependency-light validation and cleaning helper functions shared by
the Silver-layer cleaning DAG. Kept framework-free (no Airflow imports) so
they can be unit tested directly, per the project's "clean separation of
layers" principle.
"""

import hashlib
import re
from datetime import datetime, timezone

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# The three timestamp formats used across pos_sales and ecommerce_orders
# mock data (see scripts/seed_mock_sources.py). Tried in order.
TIMESTAMP_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%d-%m-%Y",
]


def parse_mixed_timestamp(value):
    """Try each known source timestamp format; return a UTC datetime or
    None if the value doesn't match any known format (itself a quality
    signal worth quarantining on)."""
    if value is None or str(value).strip() == "":
        return None
    value = str(value).strip()
    for fmt in TIMESTAMP_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def is_valid_email(value):
    if value is None or str(value).strip() == "":
        return False
    return bool(EMAIL_RE.match(str(value).strip()))


def hash_pii(value):
    """One-way hash for PII fields (email, phone) before they reach
    Silver/Gold. SHA-256 is sufficient for a local student project;
    a real system would add a per-environment salt."""
    if value is None:
        return None
    return hashlib.sha256(str(value).strip().lower().encode("utf-8")).hexdigest()


def validate_pos_sale(row):
    """Returns None if the row passes all checks, otherwise a short
    machine-readable failure reason string."""
    try:
        qty = float(row["quantity"])
    except (TypeError, ValueError):
        return "invalid_quantity_format"
    if qty <= 0:
        return "non_positive_quantity"
    try:
        price = float(row["unit_price"])
    except (TypeError, ValueError):
        return "invalid_price_format"
    if price < 0:
        return "negative_price"
    if not row.get("transaction_id"):
        return "missing_transaction_id"
    if parse_mixed_timestamp(row.get("timestamp")) is None:
        return "unparseable_timestamp"
    return None


def validate_customer(row):
    if not row.get("customer_id"):
        return "missing_customer_id"
    if not is_valid_email(row.get("email")):
        return "invalid_email_format"
    return None


def validate_ecommerce_order(row):
    if not row.get("order_id"):
        return "missing_order_id"
    if not row.get("customer_id"):
        return "missing_customer_id"
    if parse_mixed_timestamp(row.get("order_timestamp")) is None:
        return "unparseable_timestamp"
    return None


def validate_inventory_snapshot(row):
    if row.get("quantity_on_hand") is None or str(row.get("quantity_on_hand")).strip() == "":
        return "missing_quantity_on_hand"
    try:
        qty = float(row["quantity_on_hand"])
    except (TypeError, ValueError):
        return "invalid_quantity_format"
    if qty < 0:
        return "negative_quantity_on_hand"
    return None
