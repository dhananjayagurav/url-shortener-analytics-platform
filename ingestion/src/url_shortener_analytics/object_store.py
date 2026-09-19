"""Bronze-layer object storage: writing Parquet to MinIO/S3.

See docs/analytics-engineering-guide.md, "Object Storage" and "Idempotency",
for why the key format below is deterministic by design rather than
including a random run id -- it's what makes `write_bronze` safe to rerun.
"""

from __future__ import annotations

import io
import logging
import time
from datetime import datetime
from typing import Any

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from url_shortener_analytics.config import Settings
from url_shortener_analytics.exceptions import ObjectStoreWriteError

logger = logging.getLogger(__name__)


def get_s3_client(settings: Settings) -> BaseClient:
    """Build a boto3 S3 client pointed at MinIO.

    POC SIMPLIFICATION: static access-key/secret-key credentials, read from
    settings. Production equivalent: short-lived credentials from an IAM
    role (on AWS) or workload identity (on GCP/Azure) -- never long-lived
    static keys sitting in an env var, in a real deployment.
    """
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        config=Config(signature_version="s3v4", retries={"max_attempts": 0}),
    )


def build_bronze_key(table_name: str, run_date: datetime) -> str:
    """Deterministic Bronze key for a full-load snapshot.

    Deliberately keyed by (table_name, calendar date) only -- no run id, no
    timestamp-to-the-second. Two full loads of the same table on the same
    day overwrite the same object instead of producing duplicates. This is
    the idempotency mechanism for full loads; see the guide's "Idempotency"
    section and LAB 4/LAB 5 for a runnable proof.
    """
    return f"bronze/{table_name}/ingestion_date={run_date:%Y-%m-%d}/{table_name}.parquet"


def _dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()


def write_bronze(
    df: pd.DataFrame,
    table_name: str,
    run_date: datetime,
    s3_client: BaseClient,
    bucket: str,
    *,
    max_attempts: int = 3,
    backoff_seconds: float = 1.0,
) -> str:
    """Serialize `df` to Parquet and PUT it to the Bronze layer.

    Retries transient failures (network errors, throttling) up to
    `max_attempts` times with linear backoff -- a MinIO/S3 PUT is safe to
    retry blindly because the key is deterministic (build_bronze_key) and a
    retried PUT simply overwrites the same object with the same bytes.
    Raises ObjectStoreWriteError, chained from the real underlying error,
    once attempts are exhausted.
    """
    key = build_bronze_key(table_name, run_date)
    body = _dataframe_to_parquet_bytes(df)

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            s3_client.put_object(Bucket=bucket, Key=key, Body=body)
            logger.info(
                "wrote bronze object",
                extra={"key": key, "bytes": len(body), "rows": len(df), "attempt": attempt},
            )
            return key
        except (ClientError, BotoCoreError) as err:
            last_error = err
            logger.warning(
                "bronze write attempt failed, will retry" if attempt < max_attempts
                else "bronze write failed, giving up",
                extra={"key": key, "attempt": attempt, "max_attempts": max_attempts},
            )
            if attempt < max_attempts:
                time.sleep(backoff_seconds * attempt)

    raise ObjectStoreWriteError(f"failed to write s3://{bucket}/{key} after {max_attempts} attempts") from last_error


def head_object(s3_client: BaseClient, bucket: str, key: str) -> dict[str, Any] | None:
    """Return object metadata, or None if it doesn't exist. Used by tests
    and LAB exercises to verify what a run actually wrote."""
    try:
        return s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
            return None
        raise
