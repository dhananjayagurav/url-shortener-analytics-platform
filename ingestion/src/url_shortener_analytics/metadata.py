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
from datetime import UTC, datetime, timedelta
from typing import Any

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
    watermark_start: int | None = None,
    watermark_end: int | None = None,
    bronze_key: str | None = None,
) -> None:
    """Record a successful run -- including, since Section 17, the exact
    Bronze object key this run wrote (or None, for a no-op incremental run
    that wrote nothing -- see extract_incremental.run_incremental_load).
    `bronze_key` is stored explicitly rather than recomputed later from
    `source_table`/`started_at`/watermarks -- see the guide's Section 17.3
    Design Decision for why recomputation was rejected.

    `watermark_start` closes a real, previously-unnoticed gap named in
    passing back in Section 17.3: the `ingestion_metadata.watermark_start`
    column has existed in the schema since Section 13, but no code path
    ever wrote to it before Section 22 -- every incremental run's starting
    watermark was known (it's literally embedded in that run's own
    `bronze_key`, via `build_bronze_incremental_key`) but was never
    persisted as its own queryable column. See the guide's Section 22 for
    the full story and the real, genuinely-run proof that it was NULL."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE ingestion_metadata
                    SET status = 'success',
                        rows_read = :rows_read,
                        rows_written = :rows_written,
                        watermark_start = :watermark_start,
                        watermark_end = :watermark_end,
                        bronze_key = :bronze_key,
                        completed_at = :completed_at
                    WHERE run_id = :run_id
                    """
                ),
                {
                    "run_id": run_id,
                    "rows_read": rows_read,
                    "rows_written": rows_written,
                    "watermark_start": watermark_start,
                    "watermark_end": watermark_end,
                    "bronze_key": bronze_key,
                    "completed_at": datetime.now(UTC),
                },
            )
    except Exception as err:
        raise MetadataError(f"failed to record success for run {run_id}") from err

    logger.info(
        "ingestion run succeeded",
        extra={
            "run_id": run_id, "rows_written": rows_written,
            "watermark_start": watermark_start, "bronze_key": bronze_key,
        },
    )


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


def find_stale_running_runs(
    engine: Engine, *, max_runtime_minutes: int = 60, pipeline_name: str | None = None
) -> list[dict[str, Any]]:
    """Return every ingestion_metadata row still `status = 'running'` whose
    `started_at` is older than `max_runtime_minutes` ago -- almost
    certainly a crashed run (see Section 14.7/15.7's "killed mid-flight"
    failure scenarios), not one still legitimately in progress. See the
    guide's Section 16 for the full concept and why this exists.

    The cutoff is computed here, in Python, and passed as a plain
    timestamp parameter -- not `now() - interval 'N minutes'` in SQL --
    for the same cross-dialect-portability reason contracts.py computes
    type categories in Python: this function is unit-tested against
    SQLite (whose SQL dialect has no `interval` syntax at all) and used
    against Postgres in production, and a bound parameter works
    identically against both.
    """
    cutoff = datetime.now(UTC) - timedelta(minutes=max_runtime_minutes)
    query = """
        SELECT run_id, pipeline_name, source_table, load_type, started_at
        FROM ingestion_metadata
        WHERE status = 'running'
          AND started_at < :cutoff
    """
    params: dict[str, Any] = {"cutoff": cutoff}
    if pipeline_name is not None:
        query += " AND pipeline_name = :pipeline_name"
        params["pipeline_name"] = pipeline_name
    query += " ORDER BY started_at ASC"

    try:
        with engine.begin() as conn:
            rows = conn.execute(text(query), params).fetchall()
    except Exception as err:
        raise MetadataError("failed to query for stale running runs") from err

    stale = [
        {
            "run_id": row.run_id,
            "pipeline_name": row.pipeline_name,
            "source_table": row.source_table,
            "load_type": row.load_type,
            "started_at": row.started_at,
        }
        for row in rows
    ]
    if stale:
        logger.warning("stale running runs found", extra={"count": len(stale)})
    return stale


def list_successful_bronze_keys(engine: Engine, pipeline_name: str | None = None) -> list[str]:
    """Every `bronze_key` recorded by a `status = 'success'` run -- the
    control plane's own record of what it believes it wrote to Bronze.
    Used by reconciliation.py (Section 17) to compare against what
    actually exists in object storage. Excludes NULL keys (no-op
    incremental runs that wrote nothing -- see
    extract_incremental.run_incremental_load)."""
    query = "SELECT bronze_key FROM ingestion_metadata WHERE status = 'success' AND bronze_key IS NOT NULL"
    params: dict[str, Any] = {}
    if pipeline_name is not None:
        query += " AND pipeline_name = :pipeline_name"
        params["pipeline_name"] = pipeline_name

    try:
        with engine.begin() as conn:
            rows = conn.execute(text(query), params).fetchall()
    except Exception as err:
        raise MetadataError("failed to list successful bronze keys") from err

    return [row[0] for row in rows]


def get_run_history(
    engine: Engine, *, pipeline_name: str | None = None, source_table: str | None = None, limit: int = 20
) -> list[dict[str, Any]]:
    """The most recent `limit` runs, any status, most recent first -- the
    general-purpose "what actually happened" query this table has never
    had a dedicated function for before this section. Every other reader
    in this module is narrowly scoped to one status (`get_last_watermark`:
    success only; `find_stale_running_runs`: running only;
    `list_successful_bronze_keys`: success only) -- this one deliberately
    isn't, because a human debugging "why does Bronze look wrong" usually
    needs to see failures and successes side by side, in order, not one
    status at a time. See docs/analytics-engineering-guide.md, Section 22,
    for the full reasoning and LAB 17 for a genuine run against this
    sandbox's real Postgres."""
    query = "SELECT run_id, pipeline_name, source_table, load_type, status, rows_read, rows_written, bronze_key, started_at, completed_at, error_message FROM ingestion_metadata WHERE 1=1"
    params: dict[str, Any] = {"limit": limit}
    if pipeline_name is not None:
        query += " AND pipeline_name = :pipeline_name"
        params["pipeline_name"] = pipeline_name
    if source_table is not None:
        query += " AND source_table = :source_table"
        params["source_table"] = source_table
    query += " ORDER BY started_at DESC LIMIT :limit"

    try:
        with engine.begin() as conn:
            rows = conn.execute(text(query), params).fetchall()
    except Exception as err:
        raise MetadataError("failed to read run history") from err

    return [
        {
            "run_id": row.run_id, "pipeline_name": row.pipeline_name, "source_table": row.source_table,
            "load_type": row.load_type, "status": row.status, "rows_read": row.rows_read,
            "rows_written": row.rows_written, "bronze_key": row.bronze_key,
            "started_at": row.started_at, "completed_at": row.completed_at, "error_message": row.error_message,
        }
        for row in rows
    ]


def get_ingestion_summary(engine: Engine, pipeline_name: str | None = None) -> list[dict[str, Any]]:
    """One row per `source_table`: how many runs have succeeded, how many
    have failed, total rows ever written by a successful run, and the most
    recent successful run's timestamp -- an at-a-glance health summary,
    aggregated server-side in one query rather than computed by fetching
    every row and reducing it in Python (see Section 22.3's Design
    Decision for why that distinction matters here specifically)."""
    query = """
        SELECT
            source_table,
            COUNT(*) FILTER (WHERE status = 'success') AS successful_runs,
            COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
            COALESCE(SUM(rows_written) FILTER (WHERE status = 'success'), 0) AS total_rows_written,
            MAX(started_at) FILTER (WHERE status = 'success') AS last_success_at
        FROM ingestion_metadata
        WHERE 1=1
    """
    params: dict[str, Any] = {}
    if pipeline_name is not None:
        query += " AND pipeline_name = :pipeline_name"
        params["pipeline_name"] = pipeline_name
    query += " GROUP BY source_table ORDER BY source_table"

    try:
        with engine.begin() as conn:
            rows = conn.execute(text(query), params).fetchall()
    except Exception as err:
        raise MetadataError("failed to build ingestion summary") from err

    return [
        {
            "source_table": row.source_table,
            "successful_runs": row.successful_runs,
            "failed_runs": row.failed_runs,
            "total_rows_written": int(row.total_rows_written),
            "last_success_at": row.last_success_at,
        }
        for row in rows
    ]
