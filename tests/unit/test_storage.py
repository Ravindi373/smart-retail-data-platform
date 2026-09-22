"""Unit tests for src/connectors/storage.py against an in-memory S3 (moto).

These prove the two raw-layer promises from the brief: re-running a date does
not duplicate data, and raw objects are never overwritten."""
from datetime import date

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

BUCKET = "retail-raw"
RUN_DATE = date(2026, 9, 21)


@pytest.fixture()
def s3(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    with mock_aws():
        import connectors.storage as storage

        # point the connector at moto's in-process S3 instead of MinIO
        monkeypatch.setattr(
            storage, "get_s3_client",
            lambda: boto3.client("s3", region_name="us-east-1"),
        )
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield storage, client


def keys(client):
    return sorted(o["Key"] for o in client.list_objects_v2(Bucket=BUCKET).get("Contents", []))


def body(client, key):
    return client.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def test_first_landing_uses_source_and_date_folders(s3, tmp_path):
    storage, client = s3
    f = tmp_path / "pos_sales.csv"
    f.write_text("a,b\n1,2\n", newline="\n")
    key = storage.land_raw_file(str(f), "pos_sales", RUN_DATE)
    assert key == "pos_sales/2026/09/21/pos_sales.csv"
    assert keys(client) == [key]
    assert body(client, key) == b"a,b\n1,2\n"


def test_rerun_same_date_same_content_creates_nothing_new(s3, tmp_path):
    storage, client = s3
    f = tmp_path / "pos_sales.csv"
    f.write_text("a,b\n1,2\n", newline="\n")
    first = storage.land_raw_file(str(f), "pos_sales", RUN_DATE)
    second = storage.land_raw_file(str(f), "pos_sales", RUN_DATE)
    assert first == second
    assert len(keys(client)) == 1


def test_changed_content_never_overwrites_the_original(s3, tmp_path):
    storage, client = s3
    f = tmp_path / "pos_sales.csv"
    f.write_text("original\n", newline="\n")
    original_key = storage.land_raw_file(str(f), "pos_sales", RUN_DATE)

    f.write_text("changed\n", newline="\n")
    new_key = storage.land_raw_file(str(f), "pos_sales", RUN_DATE)

    assert new_key != original_key
    assert new_key.startswith("pos_sales/2026/09/21/pos_sales.") and new_key.endswith(".csv")
    assert body(client, original_key) == b"original\n"      # untouched
    assert body(client, new_key) == b"changed\n"
    assert len(keys(client)) == 2


def test_rerunning_the_changed_content_is_also_idempotent(s3, tmp_path):
    storage, client = s3
    f = tmp_path / "customers.csv"
    f.write_text("v1\n", newline="\n")
    storage.land_raw_file(str(f), "customers", RUN_DATE)
    f.write_text("v2\n", newline="\n")
    k1 = storage.land_raw_file(str(f), "customers", RUN_DATE)
    k2 = storage.land_raw_file(str(f), "customers", RUN_DATE)
    assert k1 == k2 and len(keys(client)) == 2


def test_different_dates_land_in_different_folders(s3, tmp_path):
    storage, client = s3
    f = tmp_path / "stores.csv"
    f.write_text("x\n", newline="\n")
    storage.land_raw_file(str(f), "stores", date(2026, 9, 21))
    storage.land_raw_file(str(f), "stores", date(2026, 9, 22))
    assert keys(client) == ["stores/2026/09/21/stores.csv", "stores/2026/09/22/stores.csv"]


def test_real_errors_are_raised_not_swallowed(s3, tmp_path, monkeypatch):
    storage, client = s3
    monkeypatch.setattr(storage, "RAW_BUCKET", "bucket-that-does-not-exist")
    f = tmp_path / "x.csv"
    f.write_text("x\n", newline="\n")
    with pytest.raises(ClientError):
        storage.land_raw_file(str(f), "x", RUN_DATE)