"""Incremental extraction: read only rows newer than the last successful
watermark, and land them in Bronze.

See docs/analytics-engineering-guide.md, Section 15 ("Incremental Load &
Watermarks"), for the concept, the ID-based-vs-timestamp-based design
decision, the idempotency argument for `build_bronze_incremental_key`, and
LAB 2/LAB 3.
"""

from __future__ import annotations

import logging

import pandas as pd
from botocore.client import BaseClient
from sqlalchemy import Engine, text

from url_shortener_analytics import metadata
from url_shortener_analytics.exceptions import ExtractionError
from url_shortener_analytics.object_store import write_bronze_incremental

logger = logging.getLogger(__name__)


def extract_incremental(table_name: str, engine: Engine, watermark: int) -> pd.DataFrame:
    """Read only the rows with `id > watermark`, ordered by id.

    POC SIMPLIFICATION: this assumes `table_name` has an auto-incrementing
    integer primary key named `id` that is monotonically increasing with
    insertion order -- true for every table in sql/source/, since they're
    all `BIGSERIAL PRIMARY KEY`. See the guide's "ID-based vs. Timestamp-based
    Watermark" comparison table for why this assumption is the one being
    made deliberately, and what would have to change if a table used a
    UUID primary key instead (a separate, indexed `created_at` or a
    monotonic sequence column would be required).

    `watermark` is the last id already successfully ingested -- i.e. this
    reads the open interval (watermark, +inf), not [watermark, +inf). A
    row with id == watermark was already ingested by the run that set this
    watermark, so it is deliberately excluded here to avoid re-reading it.
    """
    try:
        logger.info(
            "extracting table (incremental load)",
            extra={"table": table_name, "watermark": watermark},
        )
        query = text(f"SELECT * FROM {table_name} WHERE id > :watermark ORDER BY id")  # noqa: S608
        df = pd.read_sql_query(query, engine, params={"watermark": watermark})
    except Exception as err:
        raise ExtractionError(f"failed to extract table '{table_name}' incrementally") from err

    logger.info(
        "extraction complete",
        extra={"table": table_name, "rows": len(df), "watermark": watermark},
    )
    return df


def run_incremental_load(
    engine: Engine,
    s3_client: BaseClient,
    bucket: str,
    pipeline_name: str,
    table_name: str,
) -> dict[str, object]:
    """Checkpointed incremental-load orchestration for one table.

    Mirrors extract_full.run_full_load's checkpoint -> extract -> write ->
    checkpoint shape, with two differences that are the entire point of
    this function:

    1. The extract is scoped by a watermark read from ingestion_metadata
       (only ever from rows with status='success' -- see metadata.py's
       get_last_watermark -- so a run that itself failed can never poison
       the next run's starting point).
    2. An empty result (no new rows since the last watermark) is a normal,
       successful outcome -- not an error, and not something that should
       write a zero-row Parquet file to Bronze. See the guide's "Failure
       Scenario" subsection for why writing an empty object would be worse
       than simply skipping the write.
    """
    run_id = metadata.start_run(engine, pipeline_name, table_name, load_type="incremental")
    try:
        watermark = metadata.get_last_watermark(engine, pipeline_name, table_name)
        df = extract_incremental(table_name, engine, watermark)

        if df.empty:
            logger.info(
                "no new rows since last watermark, skipping bronze write",
                extra={"table": table_name, "watermark": watermark},
            )
            metadata.finish_run_success(
                engine, run_id, rows_read=0, rows_written=0, watermark_end=watermark
            )
            return {
                "run_id": run_id,
                "table": table_name,
                "rows": 0,
                "key": None,
                "watermark_start": watermark,
                "watermark_end": watermark,
            }

        new_watermark = int(df["id"].max())
        key = write_bronze_incremental(df, table_name, watermark, new_watermark, s3_client, bucket)
        metadata.finish_run_success(
            engine, run_id, rows_read=len(df), rows_written=len(df), watermark_end=new_watermark
        )
    except Exception as err:
        metadata.finish_run_failure(engine, run_id, str(err))
        raise

    return {
        "run_id": run_id,
        "table": table_name,
        "rows": len(df),
        "key": key,
        "watermark_start": watermark,
        "watermark_end": new_watermark,
    }
