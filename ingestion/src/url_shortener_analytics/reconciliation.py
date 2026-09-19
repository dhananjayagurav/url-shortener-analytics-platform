"""Bronze reconciliation: compare what ingestion_metadata believes this
pipeline has written against what actually exists in object storage.

See docs/analytics-engineering-guide.md, Section 17 ("Idempotency"), for
the concept and why this exists: idempotent writes (Sections 14, 15)
guarantee a *rerun* is safe, but say nothing about whether the control
plane's own record (`ingestion_metadata`) and the storage layer
(MinIO/S3) ever drift apart from each other -- from a manual write, a
retried run that landed a non-overlapping duplicate (Section 15.7's
residual edge case), or an object deleted outside this pipeline entirely.
Reconciliation is the check that catches that drift, rather than assuming
it can't happen.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from botocore.client import BaseClient
from sqlalchemy import Engine

from url_shortener_analytics import metadata
from url_shortener_analytics.object_store import head_object, list_bronze_keys


@dataclass
class ReconciliationResult:
    """orphaned: objects that exist in storage but no successful run's
    ingestion_metadata row claims to have written -- unaccounted-for data.
    missing: bronze_key values ingestion_metadata claims a successful run
    wrote, that no longer exist in storage -- an unfulfilled promise."""

    orphaned_objects: list[str] = field(default_factory=list)
    missing_objects: list[str] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not self.orphaned_objects and not self.missing_objects


def find_orphaned_bronze_objects(
    engine: Engine, s3_client: BaseClient, bucket: str, pipeline_name: str | None = None, prefix: str = "bronze/"
) -> list[str]:
    """Objects that exist in the bucket under `prefix` but aren't the
    recorded `bronze_key` of any `status='success'` run. See this
    module's docstring and Section 17.7's Failure Scenario for exactly
    when this happens in practice."""
    actual_keys = set(list_bronze_keys(s3_client, bucket, prefix))
    known_keys = set(metadata.list_successful_bronze_keys(engine, pipeline_name))
    return sorted(actual_keys - known_keys)


def find_missing_bronze_objects(
    engine: Engine, s3_client: BaseClient, bucket: str, pipeline_name: str | None = None
) -> list[str]:
    """`bronze_key` values ingestion_metadata records as successfully
    written, that no longer actually exist in the bucket (deleted
    manually, by a lifecycle policy, or by some other process entirely
    outside this pipeline's control)."""
    known_keys = metadata.list_successful_bronze_keys(engine, pipeline_name)
    return sorted(key for key in known_keys if head_object(s3_client, bucket, key) is None)


def reconcile_bronze(
    engine: Engine, s3_client: BaseClient, bucket: str, pipeline_name: str | None = None
) -> ReconciliationResult:
    """Run both checks and return a combined result. See `cli.py`'s
    `reconcile-bronze` subcommand for how this is surfaced to an
    operator."""
    return ReconciliationResult(
        orphaned_objects=find_orphaned_bronze_objects(engine, s3_client, bucket, pipeline_name),
        missing_objects=find_missing_bronze_objects(engine, s3_client, bucket, pipeline_name),
    )
