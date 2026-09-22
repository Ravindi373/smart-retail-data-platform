"""
src/quality/rules.py

Pure, dependency-light validation and cleaning helper functions shared by
the Silver-layer cleaning DAG. Kept framework-free (no Airflow imports) so
they can be unit tested directly, per the project's "clean separation of
layers" principle.

Every validate_* function returns None when the row passes, or a short,
machine-readable failure reason. That reason is what ends up in
quality.quarantine.failed_rule and on the dashboard's data quality view, so
the strings are part of the project's contract - do not rename them casually.

Rule catalogue (reason -> meaning)
    invalid_quantity_format / invalid_price_format   value is not a number
    non_positive_quantity                            quantity <= 0
    negative_price                                   unit price < 0
    zero_price                                       unit price == 0
    missing_transaction_id / missing_order_id        primary key is empty
    missing_customer_id                              required FK is empty
    unparseable_timestamp                            no known format matches
    future_dated_timestamp                           dated after the source extract
    unknown_product_identifier                       neither product_id nor SKU
                                                     matches the product master
    invalid_order_item                               an order line has a bad qty/price
    invalid_email_format                             fails the email pattern
    missing_quantity_on_hand / negative_quantity_on_hand / invalid_snapshot_date
    duplicate_business_key                           (added by the DAG, not here)
"""

import hashlib
import json
import os
import re
from datetime import datetime, timezone

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# The three timestamp formats used across pos_sales and ecommerce_orders
# mock data (see scripts/seed_mock_sources.py). Tried in order. The
# delimiters differ (T / slashes / dashes-day-first) so no value can match
# two formats - there is no day/month ambiguity between them.
TIMESTAMP_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%d-%m-%Y",
]


# --------------------------------------------------------------------------
# Cleaning helpers
# --------------------------------------------------------------------------

def parse_mixed_timestamp(value):
    """Try each known source timestamp format; return a UTC datetime or
    None if the value doesn't match any known format (itself a quality
    signal worth quarantining on).

    Assumption (documented in the report): the source systems emit UTC, so
    a parsed naive timestamp is labelled UTC rather than shifted."""
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


def clean_text(value):
    """Trim and collapse internal whitespace; empty string becomes None."""
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def clean_label(value):
    """clean_text + lower-case, for small controlled vocabularies such as
    channel, loyalty tier, payment method and order status."""
    text = clean_text(value)
    return text.lower() if text is not None else None


def normalise_key(value):
    """Canonical form of an identifier for lookups: trimmed and upper-cased
    ('sku000076 ' and 'SKU000076' are the same product)."""
    text = clean_text(value)
    return text.upper() if text is not None else None


def is_valid_email(value):
    if value is None or str(value).strip() == "":
        return False
    return bool(EMAIL_RE.match(str(value).strip()))


def hash_pii(value, salt=None):
    """One-way SHA-256 hash for PII fields (email, phone) before they reach
    Silver/Gold. Input is trimmed and lower-cased first so the same email
    always yields the same hash (joinable) regardless of formatting.

    An optional secret salt (env PII_HASH_SALT, kept out of Git) is mixed
    in so the hashes cannot be reversed with a precomputed table of common
    emails. With no salt configured the output is a plain SHA-256."""
    if value is None or str(value).strip() == "":
        return None
    if salt is None:
        salt = os.environ.get("PII_HASH_SALT", "")
    payload = f"{salt}{str(value).strip().lower()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# Reference point for "future-dated"
# --------------------------------------------------------------------------

def resolve_as_of(manifest_path=None, env=None, now=None):
    """Return (as_of_datetime_utc, origin) - the moment after which a record
    date is impossible.

    A record cannot be dated later than the extract that contains it, so the
    reference is the source extract time, resolved in this order:
      1. env SOURCE_EXTRACT_TS (ISO 8601) - explicit override
      2. extract_timestamp in the generator's _manifest.json
      3. the current time (fallback; the DAG logs a warning)
    """
    env = os.environ if env is None else env
    explicit = env.get("SOURCE_EXTRACT_TS")
    if explicit:
        return _as_utc(datetime.fromisoformat(explicit)), "env:SOURCE_EXTRACT_TS"
    if manifest_path and os.path.exists(manifest_path):
        with open(manifest_path, encoding="utf-8") as f:
            manifest = json.load(f)
        ts = manifest.get("extract_timestamp")
        if ts:
            return _as_utc(datetime.fromisoformat(ts)), "manifest"
    return _as_utc(now or datetime.now(timezone.utc)), "fallback:now"


def _as_utc(dt):
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def is_future_dated(ts, as_of):
    """True when ts (a tz-aware datetime) is later than as_of."""
    return ts is not None and as_of is not None and ts > as_of


# --------------------------------------------------------------------------
# Product identifier resolution
# --------------------------------------------------------------------------

def build_product_lookup(products):
    """Map every known identifier of a product (its product_id AND its SKU,
    normalised) to the canonical product_id. `products` is an iterable of
    dicts with product_id and sku."""
    lookup = {}
    for p in products:
        canonical = p["product_id"]
        for ident in (p.get("product_id"), p.get("sku")):
            key = normalise_key(ident)
            if key:
                lookup[key] = canonical
    return lookup


def resolve_product_id(raw, lookup):
    """Canonical product_id for whatever identifier a source system sent, or
    None when it matches nothing in the master."""
    return lookup.get(normalise_key(raw))


def resolve_order_items(items, lookup):
    """Resolve every line of an e-commerce order to canonical product ids.

    Returns (resolved_items, reason). Lines may carry `product_id` or `sku`.
    If any line is unresolvable or has a non-positive quantity/price the
    whole order is rejected (reason set, resolved_items None) - an order is
    booked or quarantined as a unit."""
    if not isinstance(items, list) or not items:
        return None, "invalid_order_item"
    resolved = []
    for item in items:
        if not isinstance(item, dict):
            return None, "invalid_order_item"
        try:
            qty = float(item.get("quantity"))
            price = float(item.get("unit_price"))
        except (TypeError, ValueError):
            return None, "invalid_order_item"
        if qty <= 0 or price <= 0:
            return None, "invalid_order_item"
        raw_id = item.get("product_id") or item.get("sku")
        canonical = resolve_product_id(raw_id, lookup)
        if canonical is None:
            return None, "unknown_product_identifier"
        resolved.append({
            "product_id": canonical,
            "quantity": item["quantity"],
            "unit_price": item["unit_price"],
        })
    return resolved, None


# --------------------------------------------------------------------------
# Row validators
# --------------------------------------------------------------------------

def validate_pos_sale(row, as_of=None, product_lookup=None):
    """Returns None if the row passes all checks, otherwise a short
    machine-readable failure reason string. as_of / product_lookup are
    optional so the function stays usable (and testable) on its own."""
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
    if price == 0:
        return "zero_price"
    if not row.get("transaction_id"):
        return "missing_transaction_id"
    ts = parse_mixed_timestamp(row.get("timestamp"))
    if ts is None:
        return "unparseable_timestamp"
    if is_future_dated(ts, as_of):
        return "future_dated_timestamp"
    if product_lookup is not None and resolve_product_id(row.get("product_id"), product_lookup) is None:
        return "unknown_product_identifier"
    return None


def validate_customer(row):
    if not row.get("customer_id"):
        return "missing_customer_id"
    if not is_valid_email(row.get("email")):
        return "invalid_email_format"
    return None


def validate_ecommerce_order(row, as_of=None, product_lookup=None):
    if not row.get("order_id"):
        return "missing_order_id"
    if not row.get("customer_id"):
        return "missing_customer_id"
    ts = parse_mixed_timestamp(row.get("order_timestamp"))
    if ts is None:
        return "unparseable_timestamp"
    if is_future_dated(ts, as_of):
        return "future_dated_timestamp"
    if product_lookup is not None:
        _, reason = resolve_order_items(row.get("items"), product_lookup)
        if reason:
            return reason
    return None


def validate_inventory_snapshot(row, product_lookup=None):
    if row.get("quantity_on_hand") is None or str(row.get("quantity_on_hand")).strip() == "":
        return "missing_quantity_on_hand"
    try:
        qty = float(row["quantity_on_hand"])
    except (TypeError, ValueError):
        return "invalid_quantity_format"
    if qty < 0:
        return "negative_quantity_on_hand"
    try:
        datetime.strptime(str(row.get("snapshot_date")), "%Y-%m-%d")
    except ValueError:
        return "invalid_snapshot_date"
    if product_lookup is not None and resolve_product_id(row.get("product_id"), product_lookup) is None:
        return "unknown_product_identifier"
    return None
