"""Shared fixtures for unit tests.

Unit tests never touch real Postgres or real MinIO -- they use an
in-memory SQLite engine (fast, zero setup) and a mocked S3 client. Tests
that need the real infrastructure live in ingestion/tests/integration/ and
are marked `@pytest.mark.integration` (see that directory's README).
"""

from __future__ import annotations

import pytest
from sqlalchemy import Engine, create_engine, text


@pytest.fixture
def sqlite_engine() -> Engine:
    """An in-memory SQLite engine with a SQLite-compatible
    ingestion_metadata table. Not a substitute for testing against real
    Postgres (see integration tests) -- this exists to unit-test this
    package's own logic (watermark math, checkpoint transitions) in
    isolation, quickly and without Docker.
    """
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE ingestion_metadata (
                run_id          TEXT PRIMARY KEY,
                pipeline_name   TEXT NOT NULL,
                source_table    TEXT NOT NULL,
                load_type       TEXT NOT NULL,
                status          TEXT NOT NULL,
                watermark_start INTEGER,
                watermark_end   INTEGER,
                rows_read       INTEGER,
                rows_written    INTEGER,
                bronze_key      TEXT,
                started_at      TIMESTAMP NOT NULL,
                completed_at    TIMESTAMP,
                error_message   TEXT
            )
            """
        ))
    return engine
