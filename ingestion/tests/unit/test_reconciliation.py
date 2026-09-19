from __future__ import annotations

from unittest.mock import MagicMock

from botocore.exceptions import ClientError
from sqlalchemy import Engine

from url_shortener_analytics import metadata
from url_shortener_analytics.reconciliation import (
    ReconciliationResult,
    find_missing_bronze_objects,
    find_orphaned_bronze_objects,
    reconcile_bronze,
)


def _record_success(engine: Engine, pipeline_name: str, table: str, bronze_key: str) -> None:
    run_id = metadata.start_run(engine, pipeline_name, table, load_type="full")
    metadata.finish_run_success(engine, run_id, rows_read=1, rows_written=1, bronze_key=bronze_key)


def test_reconciliation_result_is_clean_when_both_lists_are_empty() -> None:
    assert ReconciliationResult().clean is True


def test_reconciliation_result_is_not_clean_with_orphans_or_missing() -> None:
    assert ReconciliationResult(orphaned_objects=["x"]).clean is False
    assert ReconciliationResult(missing_objects=["y"]).clean is False


def test_find_orphaned_bronze_objects_finds_an_object_storage_does_not_know_about(sqlite_engine: Engine) -> None:
    _record_success(sqlite_engine, "pipeline_a", "clicks", "bronze/clicks/a.parquet")

    s3_client = MagicMock()
    s3_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "bronze/clicks/a.parquet"},  # known -- recorded by a successful run
            {"Key": "bronze/clicks/manual-upload.parquet"},  # orphan -- nobody recorded writing this
        ]
    }

    orphaned = find_orphaned_bronze_objects(sqlite_engine, s3_client, "test-bucket", "pipeline_a")

    assert orphaned == ["bronze/clicks/manual-upload.parquet"]


def test_find_orphaned_bronze_objects_is_empty_when_storage_matches_metadata(sqlite_engine: Engine) -> None:
    _record_success(sqlite_engine, "pipeline_a", "clicks", "bronze/clicks/a.parquet")

    s3_client = MagicMock()
    s3_client.list_objects_v2.return_value = {"Contents": [{"Key": "bronze/clicks/a.parquet"}]}

    assert find_orphaned_bronze_objects(sqlite_engine, s3_client, "test-bucket", "pipeline_a") == []


def test_find_missing_bronze_objects_finds_a_key_metadata_believes_but_storage_does_not_have(
    sqlite_engine: Engine,
) -> None:
    _record_success(sqlite_engine, "pipeline_a", "clicks", "bronze/clicks/a.parquet")
    _record_success(sqlite_engine, "pipeline_a", "clicks", "bronze/clicks/b.parquet")

    s3_client = MagicMock()

    def head_object_side_effect(Bucket: str, Key: str):  # noqa: N803
        if Key == "bronze/clicks/b.parquet":
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {"ContentLength": 1}

    s3_client.head_object.side_effect = head_object_side_effect

    missing = find_missing_bronze_objects(sqlite_engine, s3_client, "test-bucket", "pipeline_a")

    assert missing == ["bronze/clicks/b.parquet"]


def test_find_missing_bronze_objects_is_empty_when_every_recorded_key_exists(sqlite_engine: Engine) -> None:
    _record_success(sqlite_engine, "pipeline_a", "clicks", "bronze/clicks/a.parquet")

    s3_client = MagicMock()
    s3_client.head_object.return_value = {"ContentLength": 1}

    assert find_missing_bronze_objects(sqlite_engine, s3_client, "test-bucket", "pipeline_a") == []


def test_reconcile_bronze_combines_both_checks_and_is_clean_when_nothing_drifted(sqlite_engine: Engine) -> None:
    _record_success(sqlite_engine, "pipeline_a", "clicks", "bronze/clicks/a.parquet")

    s3_client = MagicMock()
    s3_client.list_objects_v2.return_value = {"Contents": [{"Key": "bronze/clicks/a.parquet"}]}
    s3_client.head_object.return_value = {"ContentLength": 1}

    result = reconcile_bronze(sqlite_engine, s3_client, "test-bucket", "pipeline_a")

    assert result.clean is True
    assert result.orphaned_objects == []
    assert result.missing_objects == []


def test_reconcile_bronze_reports_both_orphaned_and_missing_together(sqlite_engine: Engine) -> None:
    _record_success(sqlite_engine, "pipeline_a", "clicks", "bronze/clicks/a.parquet")
    _record_success(sqlite_engine, "pipeline_a", "clicks", "bronze/clicks/b.parquet")

    s3_client = MagicMock()
    # Storage has 'a' (known) and 'orphan' (unknown) -- but not 'b'.
    s3_client.list_objects_v2.return_value = {
        "Contents": [{"Key": "bronze/clicks/a.parquet"}, {"Key": "bronze/clicks/orphan.parquet"}]
    }

    def head_object_side_effect(Bucket: str, Key: str):  # noqa: N803
        if Key == "bronze/clicks/b.parquet":
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {"ContentLength": 1}

    s3_client.head_object.side_effect = head_object_side_effect

    result = reconcile_bronze(sqlite_engine, s3_client, "test-bucket", "pipeline_a")

    assert result.clean is False
    assert result.orphaned_objects == ["bronze/clicks/orphan.parquet"]
    assert result.missing_objects == ["bronze/clicks/b.parquet"]
