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


def build_bronze_incremental_key(table_name: str, watermark_start: int, watermark_end: int) -> str:
    """Deterministic Bronze key for one incremental batch.

    Keyed by (table_name, watermark_start, watermark_end) rather than by
    calendar date: an incremental pipeline can run many times a day, and
    each run's batch covers a *different* id range that must NOT overwrite
    a previous run's batch the way a full load's same-day rerun does. Two
    calls with the exact same watermark range (e.g. retrying an identical
    failed run, where no new rows arrived in between) compute the same key
    and safely overwrite each other -- see docs/analytics-engineering-guide.md,
    Section 15, "Design Decision" and "Failure Scenario" for exactly when
    this guarantee does and does not hold.
    """
    return (
        f"bronze/{table_name}/incremental/"
        f"watermark_start={watermark_start:012d}/watermark_end={watermark_end:012d}/{table_name}.parquet"
    )


def _dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()


def _put_parquet_with_retry(
    df: pd.DataFrame,
    key: str,
    s3_client: BaseClient,
    bucket: str,
    *,
    max_attempts: int,
    backoff_seconds: float,
) -> str:
    """Shared retry/serialize logic behind both write_bronze and
    write_bronze_incremental -- the only thing that differs between a full
    load's write and an incremental load's write is how the key is built,
    not how the write itself is performed or retried."""
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
    """Serialize `df` to Parquet and PUT it to the Bronze layer at the
    deterministic full-load key. See build_bronze_key's docstring for the
    idempotency argument; raises ObjectStoreWriteError once retries are
    exhausted."""
    key = build_bronze_key(table_name, run_date)
    return _put_parquet_with_retry(df, key, s3_client, bucket, max_attempts=max_attempts, backoff_seconds=backoff_seconds)


def write_bronze_incremental(
    df: pd.DataFrame,
    table_name: str,
    watermark_start: int,
    watermark_end: int,
    s3_client: BaseClient,
    bucket: str,
    *,
    max_attempts: int = 3,
    backoff_seconds: float = 1.0,
) -> str:
    """Serialize `df` to Parquet and PUT it to the Bronze layer at the
    deterministic incremental-batch key. See build_bronze_incremental_key's
    docstring for exactly what "deterministic" does and doesn't guarantee
    here."""
    key = build_bronze_incremental_key(table_name, watermark_start, watermark_end)
    return _put_parquet_with_retry(df, key, s3_client, bucket, max_attempts=max_attempts, backoff_seconds=backoff_seconds)


def head_object(s3_client: BaseClient, bucket: str, key: str) -> dict[str, Any] | None:
    """Return object metadata, or None if it doesn't exist. Used by tests
    and LAB exercises to verify what a run actually wrote."""
    try:
        return s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
            return None
        raise
