"""Full-load extraction: read an entire source table and land it in Bronze.

See docs/analytics-engineering-guide.md, "Full Load Ingestion", for the
concept, the idempotency argument, and LAB 1.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import pandas as pd
from botocore.client import BaseClient
from sqlalchemy import Engine

from url_shortener_analytics import metadata
from url_shortener_analytics.exceptions import ExtractionError
from url_shortener_analytics.object_store import write_bronze

logger = logging.getLogger(__name__)


def extract_full(table_name: str, engine: Engine) -> pd.DataFrame:
    """Read an entire table into memory.

    POC SIMPLIFICATION: this reads the whole table in one query via
    `pd.read_sql_table`. Fine for the `urls`/`users` table sizes this repo
    seeds locally; wrong for a production table with hundreds of millions
    of rows, which would exhaust memory and hold a long-running scan open
    against the OLTP database. Production equivalent: page through the
    table in bounded chunks (`pd.read_sql_query` with LIMIT/OFFSET, or a
    server-side cursor), writing each chunk as its own Parquet file. See
    the guide's "Production Considerations" table for this component.
    """
    try:
        logger.info("extracting table (full load)", extra={"table": table_name})
        df = pd.read_sql_table(table_name, engine)
    except Exception as err:
        raise ExtractionError(f"failed to extract table '{table_name}'") from err

    logger.info(
        "extraction complete", extra={"table": table_name, "rows": len(df), "columns": len(df.columns)}
    )
    return df


def run_full_load(
    engine: Engine,
    s3_client: BaseClient,
    bucket: str,
    pipeline_name: str,
    table_name: str,
) -> dict[str, object]:
    """End-to-end full load for one table: checkpoint -> extract -> write ->
    checkpoint. This is the function `cli.py`'s `full-load` subcommand
    calls once per configured table.

    On any failure, the run is recorded as `failed` in ingestion_metadata
    (never left `running`) and the exception is re-raised -- callers (the
    CLI, or an orchestrator in a later phase) decide whether one table's
    failure should stop the whole batch.
    """
    run_id = metadata.start_run(engine, pipeline_name, table_name, load_type="full")
    try:
        df = extract_full(table_name, engine)
        run_date = datetime.now(UTC)
        key = write_bronze(df, table_name, run_date, s3_client, bucket)
        metadata.finish_run_success(engine, run_id, rows_read=len(df), rows_written=len(df))
    except Exception as err:
        metadata.finish_run_failure(engine, run_id, str(err))
        raise

    return {"run_id": run_id, "table": table_name, "rows": len(df), "key": key}
