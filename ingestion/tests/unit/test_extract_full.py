from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from sqlalchemy import Engine

from url_shortener_analytics.exceptions import ExtractionError, ObjectStoreWriteError
from url_shortener_analytics.extract_full import extract_full, run_full_load
from url_shortener_analytics import metadata


@pytest.fixture
def seeded_table(sqlite_engine: Engine) -> Engine:
    df = pd.DataFrame({"id": [1, 2, 3], "short_code": ["abc", "def", "ghi"]})
    df.to_sql("urls", sqlite_engine, if_exists="replace", index=False)
    return sqlite_engine


def test_extract_full_reads_the_whole_table(seeded_table: Engine) -> None:
    df = extract_full("urls", seeded_table)
    assert len(df) == 3
    assert list(df["short_code"]) == ["abc", "def", "ghi"]


def test_extract_full_wraps_errors_from_a_missing_table(sqlite_engine: Engine) -> None:
    with pytest.raises(ExtractionError):
        extract_full("table_that_does_not_exist", sqlite_engine)


def test_run_full_load_checkpoints_success(seeded_table: Engine) -> None:
    s3_client = MagicMock()

    result = run_full_load(seeded_table, s3_client, "test-bucket", "pipeline_a", "urls")

    assert result["rows"] == 3
    assert result["key"] is not None
    assert s3_client.put_object.call_count == 1
    with seeded_table.begin() as conn:
        from sqlalchemy import text
        row = conn.execute(text(
            "SELECT status, rows_written, bronze_key FROM ingestion_metadata WHERE run_id = :run_id"
        ), {"run_id": result["run_id"]}).fetchone()
    assert row.status == "success"
    assert row.rows_written == 3
    # The exact key this run wrote to Bronze is persisted on the checkpoint
    # row -- see metadata.finish_run_success and Section 17's Design
    # Decision for why it's stored rather than recomputed later.
    assert row.bronze_key == result["key"]


def test_run_full_load_checkpoints_failure_and_reraises(seeded_table: Engine) -> None:
    s3_client = MagicMock()
    s3_client.put_object.side_effect = ClientError(
        {"Error": {"Code": "EndpointConnectionError", "Message": "MinIO is unreachable"}}, "PutObject"
    )

    with pytest.raises(ObjectStoreWriteError):
        run_full_load(seeded_table, s3_client, "test-bucket", "pipeline_a", "urls")

    # The run must be recorded as failed, not left `running` forever.
    assert metadata.get_last_watermark(seeded_table, "pipeline_a", "urls") == 0
    with seeded_table.begin() as conn:
        from sqlalchemy import text
        row = conn.execute(text(
            "SELECT status FROM ingestion_metadata WHERE source_table = 'urls' ORDER BY started_at DESC LIMIT 1"
        )).fetchone()
    assert row.status == "failed"
