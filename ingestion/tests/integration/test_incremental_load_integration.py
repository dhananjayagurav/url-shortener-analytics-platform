"""Integration test: a real incremental load of `clicks` against the real
Postgres + MinIO this repo's docker-compose.yml brings up. Requires:

    docker compose up -d
    make test-integration

These are NOT run by `make test` / plain `pytest` -- see pyproject.toml's
`addopts = "-m 'not integration'"` and ingestion/tests/integration/README.md
for why, and for the exact commands to run this file specifically.

NOT YET EXECUTED in this sandbox -- there is no Docker daemon available
here (`docker info` fails; see the guide's Section 15 "How to Verify" for
exactly what output running this for real should produce). Written to be
correct and runnable on a machine with Docker, not run-and-observed here.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from url_shortener_analytics.config import Settings
from url_shortener_analytics.db import engine_from_settings
from url_shortener_analytics.extract_incremental import run_incremental_load
from url_shortener_analytics.object_store import get_s3_client, head_object


@pytest.fixture
def settings() -> Settings:
    return Settings()


def _insert_click(engine, short_code: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO clicks (short_code, device_type) VALUES (:short_code, 'desktop')"),
            {"short_code": short_code},
        )


@pytest.mark.integration
def test_incremental_load_reads_only_rows_after_the_watermark(settings: Settings) -> None:
    """LAB 2 as an assertion: a second run must only pick up rows inserted
    after the first run's watermark, not the whole table again."""
    engine = engine_from_settings(settings)
    s3_client = get_s3_client(settings)
    pipeline_name = "integration_test_incremental"

    _insert_click(engine, "aaa111")
    result_1 = run_incremental_load(engine, s3_client, settings.minio_bucket, pipeline_name, "clicks")
    assert result_1["rows"] >= 1

    _insert_click(engine, "bbb222")
    _insert_click(engine, "ccc333")
    result_2 = run_incremental_load(engine, s3_client, settings.minio_bucket, pipeline_name, "clicks")

    assert result_2["rows"] == 2
    assert result_2["watermark_start"] == result_1["watermark_end"]
    assert result_2["watermark_end"] > result_1["watermark_end"]

    obj = head_object(s3_client, settings.minio_bucket, result_2["key"])
    assert obj is not None, "the Bronze object run_incremental_load reported writing should actually exist in MinIO"


@pytest.mark.integration
def test_incremental_load_with_no_new_rows_writes_nothing(settings: Settings) -> None:
    """LAB 3 as an assertion: running twice in a row with no new rows in
    between must not write a second (empty) Bronze object."""
    engine = engine_from_settings(settings)
    s3_client = get_s3_client(settings)
    pipeline_name = "integration_test_incremental_empty"

    _insert_click(engine, "ddd444")
    result_1 = run_incremental_load(engine, s3_client, settings.minio_bucket, pipeline_name, "clicks")
    result_2 = run_incremental_load(engine, s3_client, settings.minio_bucket, pipeline_name, "clicks")

    assert result_2["rows"] == 0
    assert result_2["key"] is None
    assert result_2["watermark_start"] == result_2["watermark_end"] == result_1["watermark_end"]


@pytest.mark.integration
def test_retrying_the_same_failed_watermark_range_overwrites_not_duplicates(settings: Settings) -> None:
    """See docs/analytics-engineering-guide.md Section 15, "Failure Scenario":
    if a run's Bronze write fails and it is retried with NO new rows having
    arrived in between, the retry computes the exact same
    (watermark_start, watermark_end) key and overwrites cleanly -- there is
    only ever one object for that exact range, never two."""
    engine = engine_from_settings(settings)
    s3_client = get_s3_client(settings)
    pipeline_name = "integration_test_incremental_retry"

    _insert_click(engine, "eee555")
    result_1 = run_incremental_load(engine, s3_client, settings.minio_bucket, pipeline_name, "clicks")

    # Simulate "retrying the same range": call the same watermark-scoped
    # write function directly with the same start/end, exactly as a retry
    # of a run that failed AFTER extracting but BEFORE the metadata
    # checkpoint would do.
    from url_shortener_analytics.extract_incremental import extract_incremental
    from url_shortener_analytics.object_store import write_bronze_incremental

    df = extract_incremental("clicks", engine, watermark=0)
    key_retry = write_bronze_incremental(
        df, "clicks", 0, result_1["watermark_end"], s3_client, settings.minio_bucket
    )

    assert key_retry == result_1["key"]
    prefix = result_1["key"].rsplit("/", 1)[0] + "/"
    listing = s3_client.list_objects_v2(Bucket=settings.minio_bucket, Prefix=prefix)
    assert listing.get("KeyCount", 0) == 1
