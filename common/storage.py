"""S3 object-storage helpers, shared across phases.

Used by the tariff generator (Phase 1), the Spark jobs (Phase 2) and the billing
report writer (Phase 4). Nothing here knows it is talking to SeaweedFS -- it is
plain S3, which is what made replacing MinIO a compose-file change.
"""

from __future__ import annotations

import boto3
from botocore.client import Config as BotoConfig


def s3_client(cfg):
    """A boto3 S3 client pointed at the configured endpoint."""
    access_key, secret_key = cfg.s3_credentials
    return boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=cfg.s3_region,
        # Path-style addressing is required: virtual-host style would resolve
        # `raw.seaweedfs`, which does not exist on the compose network. Real AWS
        # accepts path-style too, so this stays correct against S3 proper.
        config=BotoConfig(s3={"addressing_style": "path"}),
    )


def put_text(client, bucket: str, key: str, text: str) -> None:
    client.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))


def get_text(client, bucket: str, key: str) -> str:
    return client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")


def list_keys(client, bucket: str, prefix: str = "") -> list[str]:
    """Every key under a prefix, following pagination."""
    keys: list[str] = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
        if not page.get("IsTruncated"):
            return sorted(keys)
        token = page.get("NextContinuationToken")
