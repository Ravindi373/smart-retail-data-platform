"""
src/connectors/storage.py

Thin wrapper around boto3's S3 client pointed at MinIO. Used by the
Bronze ingestion DAG to land raw files under date-partitioned paths
(<bucket>/<source>/YYYY/MM/DD/<filename>) per the project's architecture
rule that Raw data is never edited after landing.

Raw immutability and idempotency (both come from the same content check):
  * Same (source, date), identical content  -> nothing is uploaded; the
    existing object key is returned. Re-running a DAG for a date never
    duplicates raw data.
  * Same (source, date), DIFFERENT content   -> the original object is left
    untouched and the new content is landed next to it as
    <stem>.<8-char content hash><ext>. Nothing is ever overwritten, so a
    wrong cleaning rule can always be re-run from the original files.
"""

import hashlib
import logging
import os

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

RAW_BUCKET = os.environ.get("RAW_BUCKET", "retail-raw")
_NOT_FOUND = {"404", "NoSuchKey", "NotFound"}


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
    return hashlib.md5(data).hexdigest()  # content fingerprint, not security


def _stored_hash(client, key):
    """Return (exists, stored_content_hash). A 404 means 'not there yet';
    any other error (bad credentials, network) is raised, not swallowed."""
    try:
        head = client.head_object(Bucket=RAW_BUCKET, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code", "") in _NOT_FOUND:
            return False, None
        raise
    return True, head.get("Metadata", {}).get("content-md5")


def land_raw_file(local_path: str, source_name: str, run_date) -> str:
    """Land local_path under <source_name>/<YYYY>/<MM>/<DD>/ and return the
    object key. See the module docstring for the idempotency rules."""
    client = get_s3_client()
    filename = os.path.basename(local_path)
    date_prefix = run_date.strftime("%Y/%m/%d")
    key = f"{source_name}/{date_prefix}/{filename}"

    with open(local_path, "rb") as f:
        data = f.read()
    local_hash = _md5_of_bytes(data)

    exists, stored = _stored_hash(client, key)
    if not exists:
        _put(client, key, data, local_hash)
        log.info("landed %s (%d bytes)", key, len(data))
        return key
    if stored == local_hash:
        log.info("skipped %s - identical content already landed", key)
        return key

    stem, ext = os.path.splitext(filename)
    versioned_key = f"{source_name}/{date_prefix}/{stem}.{local_hash[:8]}{ext}"
    exists, stored = _stored_hash(client, versioned_key)
    if not (exists and stored == local_hash):
        _put(client, versioned_key, data, local_hash)
        log.warning(
            "content changed for %s on %s - original kept, new version landed as %s",
            source_name, date_prefix, versioned_key,
        )
    return versioned_key


def _put(client, key, data, local_hash):
    client.put_object(
        Bucket=RAW_BUCKET, Key=key, Body=data, Metadata={"content-md5": local_hash}
    )
