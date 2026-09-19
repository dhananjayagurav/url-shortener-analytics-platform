"""Ingestion run metadata: watermark, checkpoint, and run history, combined
into one table (see sql/source/003_ingestion_metadata.sql).

See docs/analytics-engineering-guide.md, "Checkpoints and Watermarks", for
the terminology this module implements:

- watermark: where does the *next* incremental run resume from?
- checkpoint: did *this specific* run complete, and can its output be
  trusted? (the `status` column)
- run metadata: what happened during this run, for debugging/audit?

Only `status = 'success'` rows are ever read back as a watermark source
(get_last_watermark) -- a run that crashed mid-flight must never become the
basis for the next run's "where do I resume" decision. See the guide's
failure-scenario walkthrough for exactly what goes wrong if that rule is
skipped.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime

from sqlalchemy import Engine, text

from url_shortener_analytics.exceptions import MetadataError

logger = logging.getLogger(__name__)


def start_run(engine: Engine, pipeline_name: str, source_table: str, load_type: str) -> str:
    """Insert a `running` checkpoint row before any extraction happens.
    Returns the new run_id (a UUID string)."""
    run_id = str(uuid.uuid4())
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO ingestion_metadata
                        (run_id, pipeline_name, source_table, load_type, status, started_at)
                    VALUES
                        (:run_id, :pipeline_name, :source_table, :load_type, 'running', :started_at)
                    """
                ),
                {
                    "run_id": run_id,
                    "pipeline_name": pipeline_name,
                    "source_table": source_table,
                    "load_type": load_type,
                    "started_at": datetime.now(UTC),
                },
            )
    except Exception as err:
        raise MetadataError(f"failed to start run for {source_table}") from err

    logger.info(
        "ingestion run started",
        extra={"run_id": run_id, "source_table": source_table, "load_type": load_type},
    )
    return run_id


def finish_run_success(
    engine: Engine,
    run_id: str,
    *,
    rows_read: int,
    rows_written: int,
    watermark_end: int | None = None,
) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE ingestion_metadata
                    SET status = 'success',
                        rows_read = :rows_read,
                        rows_written = :rows_written,
                        watermark_end = :watermark_end,
                        completed_at = :completed_at
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "run_id": run_id,
                    "rows_read": rows_read,
                    "rows_written": rows_written,
                    "watermark_end": watermark_end,
                    "completed_at": datetime.now(UTC),
                },
            )
    except Exception as err:
        raise MetadataError(f"failed to record success for run {run_id}") from err

    logger.info("ingestion run succeeded", extra={"run_id": run_id, "rows_written": rows_written})


def finish_run_failure(engine: Engine, run_id: str, error_message: str) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE ingestion_metadata
                    SET status = 'failed',
                        error_message = :error_message,
                        completed_at = :completed_at
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "run_id": run_id,
                    "error_message": error_message[:2000],
                    "completed_at": datetime.now(UTC),
                },
            )
    except Exception as err:
        raise MetadataError(f"failed to record failure for run {run_id}") from err

    logger.warning("ingestion run failed", extra={"run_id": run_id, "error": error_message[:200]})


def get_last_watermark(engine: Engine, pipeline_name: str, source_table: str) -> int:
    """Highest watermark_end from the most recent *successful* run, or 0 if
    there has never been one. 0 as the "never run" default means the first
    incremental run naturally reads everything (`WHERE id > 0`), with no
    separate first-run code path needed."""
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT watermark_end
                    FROM ingestion_metadata
                    WHERE pipeline_name = :pipeline_name
                      AND source_table = :source_table
                      AND status = 'success'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                ),
                {"pipeline_name": pipeline_name, "source_table": source_table},
            ).fetchone()
    except Exception as err:
        raise MetadataError(f"failed to read watermark for {source_table}") from err

    watermark = int(row[0]) if row and row[0] is not None else 0
    logger.debug("watermark read", extra={"source_table": source_table, "watermark": watermark})
    return watermark
