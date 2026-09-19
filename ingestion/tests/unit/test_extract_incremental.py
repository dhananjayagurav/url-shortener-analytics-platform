from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from sqlalchemy import Engine, text

from url_shortener_analytics import metadata
from url_shortener_analytics.exceptions import ExtractionError, ObjectStoreWriteError
from url_shortener_analytics.extract_incremental import extract_incremental, run_incremental_load


@pytest.fixture
def seeded_clicks(sqlite_engine: Engine) -> Engine:
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "url_id": [1, 1, 2, 2, 3]})
    df.to_sql("clicks", sqlite_engine, if_exists="replace", index=False)
    return sqlite_engine


def test_extract_incremental_reads_only_rows_after_the_watermark(seeded_clicks: Engine) -> None:
    df = extract_incremental("clicks", seeded_clicks, watermark=2)
    assert list(df["id"]) == [3, 4, 5]


def test_extract_incremental_watermark_zero_reads_everything(seeded_clicks: Engine) -> None:
    df = extract_incremental("clicks", seeded_clicks, watermark=0)
    assert list(df["id"]) == [1, 2, 3, 4, 5]


def test_extract_incremental_watermark_at_max_id_reads_nothing(seeded_clicks: Engine) -> None:
    df = extract_incremental("clicks", seeded_clicks, watermark=5)
    assert df.empty


def test_extract_incremental_wraps_errors_from_a_missing_table(sqlite_engine: Engine) -> None:
    with pytest.raises(ExtractionError):
        extract_incremental("table_that_does_not_exist", sqlite_engine, watermark=0)


def test_run_incremental_load_first_run_reads_everything_and_advances_watermark(seeded_clicks: Engine) -> None:
    s3_client = MagicMock()

    result = run_incremental_load(seeded_clicks, s3_client, "test-bucket", "pipeline_a", "clicks")

    assert result["rows"] == 5
    assert result["watermark_start"] == 0
    assert result["watermark_end"] == 5
    assert result["key"] is not None
    assert s3_client.put_object.call_count == 1
    assert metadata.get_last_watermark(seeded_clicks, "pipeline_a", "clicks") == 5
    with seeded_clicks.begin() as conn:
        row = conn.execute(text(
            "SELECT bronze_key FROM ingestion_metadata WHERE run_id = :run_id"
        ), {"run_id": result["run_id"]}).fetchone()
    assert row.bronze_key == result["key"]


def test_run_incremental_load_second_run_only_reads_new_rows(seeded_clicks: Engine) -> None:
    s3_client = MagicMock()
    run_incremental_load(seeded_clicks, s3_client, "test-bucket", "pipeline_a", "clicks")

    with seeded_clicks.begin() as conn:
        conn.execute(text("INSERT INTO clicks (id, url_id) VALUES (6, 3), (7, 1)"))

    result = run_incremental_load(seeded_clicks, s3_client, "test-bucket", "pipeline_a", "clicks")

    assert result["rows"] == 2
    assert result["watermark_start"] == 5
    assert result["watermark_end"] == 7
    assert s3_client.put_object.call_count == 2
    assert metadata.get_last_watermark(seeded_clicks, "pipeline_a", "clicks") == 7


def test_run_incremental_load_with_no_new_rows_skips_the_write_but_still_succeeds(seeded_clicks: Engine) -> None:
    s3_client = MagicMock()
    run_incremental_load(seeded_clicks, s3_client, "test-bucket", "pipeline_a", "clicks")

    result = run_incremental_load(seeded_clicks, s3_client, "test-bucket", "pipeline_a", "clicks")

    assert result["rows"] == 0
    assert result["key"] is None
    assert result["watermark_start"] == result["watermark_end"] == 5
    # Still only ONE put_object call total (from the first run) -- the
    # second, empty run must not write a zero-row Parquet object.
    assert s3_client.put_object.call_count == 1
    with seeded_clicks.begin() as conn:
        row = conn.execute(text(
            "SELECT status, rows_written, bronze_key FROM ingestion_metadata WHERE run_id = :run_id"
        ), {"run_id": result["run_id"]}).fetchone()
    assert row.status == "success"
    assert row.rows_written == 0
    # A no-op run wrote nothing to Bronze, so bronze_key must stay NULL --
    # not some recomputed key for an object that doesn't exist.
    assert row.bronze_key is None


def test_run_incremental_load_checkpoints_failure_and_reraises_without_advancing_watermark(
    seeded_clicks: Engine,
) -> None:
    s3_client = MagicMock()
    s3_client.put_object.side_effect = ClientError(
        {"Error": {"Code": "EndpointConnectionError", "Message": "MinIO is unreachable"}}, "PutObject"
    )

    with pytest.raises(ObjectStoreWriteError):
        run_incremental_load(seeded_clicks, s3_client, "test-bucket", "pipeline_a", "clicks")

    # The failed run must not have moved the watermark -- the next run must
    # still start from 0, or it would silently skip the rows this run
    # failed to land in Bronze.
    assert metadata.get_last_watermark(seeded_clicks, "pipeline_a", "clicks") == 0
    with seeded_clicks.begin() as conn:
        row = conn.execute(text(
            "SELECT status FROM ingestion_metadata WHERE source_table = 'clicks' ORDER BY started_at DESC LIMIT 1"
        )).fetchone()
    assert row.status == "failed"
