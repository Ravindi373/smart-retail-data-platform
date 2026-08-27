"""
src/connectors/storage.py

Thin wrapper around boto3's S3 client pointed at MinIO. Used by the
Bronze ingestion DAG to land raw files under date-partitioned paths
(raw/<source>/YYYY/MM/DD/<filename>) per the project's architecture rule
that Raw data is never edited after landing.

Idempotency: uploads are keyed by a fixed, deterministic object path for
a given (source, run_date), and skipped if an object with the same
content hash already exists at that path — so re-running a DAG for the
same date does not create duplicate raw objects or unnecessary re-uploads.
"""

import hashlib
import os
import boto3
from botocore.client import Config

RAW_BUCKET = "retail-raw"


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _md5_of_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def land_raw_file(local_path: str, source_name: str, run_date) -> str:
    """Uploads local_path to raw/<source_name>/<YYYY>/<MM>/<DD>/<filename>.
    Skips the upload if an object already exists at that key with
    identical content (idempotent rerun for the same date). Returns the
    object key."""
    client = get_s3_client()
    filename = os.path.basename(local_path)
    date_prefix = run_date.strftime("%Y/%m/%d")
    key = f"{source_name}/{date_prefix}/{filename}"

    with open(local_path, "rb") as f:
        data = f.read()
    local_hash = _md5_of_bytes(data)

    try:
        head = client.head_object(Bucket=RAW_BUCKET, Key=key)
        existing_hash = head.get("Metadata", {}).get("content-md5")
        if existing_hash == local_hash:
            return key  # already landed for this date, identical content
    except client.exceptions.ClientError:
        pass  # object doesn't exist yet — proceed to upload

    client.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=data,
        Metadata={"content-md5": local_hash},
    )
    return key
