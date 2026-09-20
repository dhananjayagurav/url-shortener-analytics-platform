"""Command-line entrypoint: `python -m url_shortener_analytics.cli <command>`
(or the `ingest` console script installed by pyproject.toml).

Ten commands:

- `run`                -- dispatch each table in pipelines.yaml to full or
                          incremental load, per its configured `load_type`.
                          This is what a scheduled/production run should
                          call.
- `full-load`          -- force a full load of every configured table,
                          regardless of its configured `load_type`. Kept as
                          an explicit operator override for
                          backfills/rebuilds -- see the guide's Section 15,
                          "Production Considerations", for when you'd reach
                          for this over `run`.
- `validate-contracts` -- check every contract in contracts/source/*.yaml
                          against the real, connected database. See the
                          guide's Section 12.
- `check-stale-runs`   -- report any ingestion_metadata row still
                          `status='running'` past a reasonable max runtime
                          -- almost certainly a crashed run. See the
                          guide's Section 16.
- `reconcile-bronze`   -- compare ingestion_metadata's record of what it
                          wrote against what actually exists in object
                          storage, reporting orphaned and missing objects.
                          See the guide's Section 17.
- `storage-stats`      -- report object count and total bytes under a
                          Bronze prefix -- a cheap capacity/growth signal.
                          See the guide's Section 18.
- `layout-report`      -- per-table file-layout health: object count,
                          average/min/max size, and how many objects are
                          "small files". See the guide's Section 21.
- `ingestion-history`  -- the most recent N runs, any status, most recent
                          first -- the general-purpose "what actually
                          happened" query. See the guide's Section 22.
- `ingestion-summary`  -- one row per source table: successful/failed run
                          counts, total rows written, last success time.
                          See the guide's Section 22.
- `pii-report`         -- every column any contract in contracts/source/
                          classifies as carrying personal data, and at
                          what sensitivity. See the guide's Section 23.

See docs/analytics-engineering-guide.md, "Full Load Ingestion -> How to
Run", Section 15 "How to Run", Section 12 "How to Run", Section 16 "How to
Run", Section 17 "How to Run", Section 18 "How to Run", Section 21 "How to
Run", Section 22 "How to Run", and Section 23 "How to Run", for the exact
commands and expected output.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import yaml

from url_shortener_analytics import metadata
from url_shortener_analytics.config import get_settings
from url_shortener_analytics.contracts import DEFAULT_CONTRACTS_DIR, validate_all_contracts
from url_shortener_analytics.db import engine_from_settings
from url_shortener_analytics.exceptions import ContractError
from url_shortener_analytics.extract_full import run_full_load
from url_shortener_analytics.extract_incremental import run_incremental_load
from url_shortener_analytics.logging_setup import configure_logging
from url_shortener_analytics.object_store import get_bucket_stats, get_file_layout_report, get_s3_client
from url_shortener_analytics.pii import classify_all_contracts
from url_shortener_analytics.reconciliation import reconcile_bronze

logger = logging.getLogger(__name__)

DEFAULT_PIPELINE_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "pipelines.yaml"


def _load_pipeline_config(path: Path) -> dict:
    with path.open() as f:
        return yaml.safe_load(f)


def run_full_load_command(config_path: Path = DEFAULT_PIPELINE_CONFIG) -> int:
    """Force a full load of every table in `config_path`, ignoring each
    table's configured `load_type`. See module docstring for when this is
    the right command instead of `run`."""
    settings = get_settings()
    configure_logging(settings.log_level)

    config = _load_pipeline_config(config_path)
    pipeline_name = config["pipeline_name"]
    tables = [t["name"] for t in config["tables"]]

    engine = engine_from_settings(settings)
    s3_client = get_s3_client(settings)

    logger.info("starting full load (forced for all tables)", extra={"pipeline": pipeline_name, "tables": tables})

    failures: list[str] = []
    for table_name in tables:
        try:
            result = run_full_load(engine, s3_client, settings.minio_bucket, pipeline_name, table_name)
            logger.info("table done", extra=result)
        except Exception:
            logger.exception("table failed", extra={"table": table_name})
            failures.append(table_name)

    if failures:
        logger.error("full load finished with failures", extra={"failed_tables": failures})
        return 1

    logger.info("full load finished successfully")
    return 0


def run_command(config_path: Path = DEFAULT_PIPELINE_CONFIG) -> int:
    """Dispatch each table in `config_path` to full or incremental load,
    per its configured `load_type`. This is the normal, day-to-day entry
    point -- see docs/analytics-engineering-guide.md Section 15 for why
    the dispatch decision lives in config rather than in this function as
    an if/else per table name."""
    settings = get_settings()
    configure_logging(settings.log_level)

    config = _load_pipeline_config(config_path)
    pipeline_name = config["pipeline_name"]
    tables = config["tables"]

    engine = engine_from_settings(settings)
    s3_client = get_s3_client(settings)

    logger.info(
        "starting pipeline run",
        extra={"pipeline": pipeline_name, "tables": [t["name"] for t in tables]},
    )

    failures: list[str] = []
    for table in tables:
        table_name = table["name"]
        load_type = table.get("load_type", "full")
        try:
            if load_type == "full":
                result = run_full_load(engine, s3_client, settings.minio_bucket, pipeline_name, table_name)
            elif load_type == "incremental":
                result = run_incremental_load(engine, s3_client, settings.minio_bucket, pipeline_name, table_name)
            else:
                raise ValueError(f"unknown load_type '{load_type}' for table '{table_name}'")
            logger.info("table done", extra={"load_type": load_type, **result})
        except Exception:
            logger.exception("table failed", extra={"table": table_name, "load_type": load_type})
            failures.append(table_name)

    if failures:
        logger.error("pipeline run finished with failures", extra={"failed_tables": failures})
        return 1

    logger.info("pipeline run finished successfully")
    return 0


def validate_contracts_command(contracts_dir: Path = DEFAULT_CONTRACTS_DIR) -> int:
    """Validate every contract in `contracts_dir` against the real,
    connected database. Prints a pass/fail line per table and every
    violation/warning found; exits 1 if any table has at least one
    violation (warnings alone don't fail the run -- see contracts.py's
    module docstring)."""
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = engine_from_settings(settings)

    try:
        results = validate_all_contracts(engine, contracts_dir)
    except ContractError:
        logger.exception("contract validation could not be completed")
        return 1

    any_failed = False
    for result in results:
        if result.passed:
            logger.info("contract passed", extra={"table": result.table, "warnings": len(result.warnings)})
        else:
            any_failed = True
            logger.error(
                "contract FAILED", extra={"table": result.table, "violations": len(result.violations)}
            )
        for violation in result.violations:
            logger.error("violation", extra={"table": result.table, "kind": violation.kind, "column": violation.column, "detail": violation.detail})
        for warning in result.warnings:
            logger.warning("warning", extra={"table": result.table, "kind": warning.kind, "column": warning.column, "detail": warning.detail})

    if any_failed:
        logger.error("contract validation finished with failures")
        return 1

    logger.info("all contracts passed")
    return 0


def check_stale_runs_command(max_runtime_minutes: int = 60) -> int:
    """Report any ingestion_metadata row still `status='running'` past
    `max_runtime_minutes` -- almost certainly a crashed run, not one
    still legitimately in progress. Prints one line per stale run found;
    exits 1 if any were found (so this is safe to wire into a monitoring
    cron's alerting, per the guide's Section 16, "Production
    Considerations")."""
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = engine_from_settings(settings)

    stale = metadata.find_stale_running_runs(engine, max_runtime_minutes=max_runtime_minutes)

    if not stale:
        logger.info("no stale running runs found", extra={"max_runtime_minutes": max_runtime_minutes})
        return 0

    for run in stale:
        logger.warning(
            "stale running run",
            extra={
                "run_id": run["run_id"],
                "pipeline_name": run["pipeline_name"],
                "source_table": run["source_table"],
                "load_type": run["load_type"],
                "started_at": str(run["started_at"]),
            },
        )
    logger.error("stale running runs found", extra={"count": len(stale)})
    return 1


def reconcile_bronze_command(config_path: Path = DEFAULT_PIPELINE_CONFIG) -> int:
    """Compare ingestion_metadata's record of what it wrote against what
    actually exists in Bronze object storage. Prints every orphaned and
    missing object found; exits 1 unless the result is clean. See the
    guide's Section 17, "How to Run" and "Failure Scenario"."""
    settings = get_settings()
    configure_logging(settings.log_level)

    config = _load_pipeline_config(config_path)
    pipeline_name = config["pipeline_name"]

    engine = engine_from_settings(settings)
    s3_client = get_s3_client(settings)

    result = reconcile_bronze(engine, s3_client, settings.minio_bucket, pipeline_name)

    for key in result.orphaned_objects:
        logger.warning("orphaned bronze object", extra={"key": key})
    for key in result.missing_objects:
        logger.warning("missing bronze object", extra={"key": key})

    if not result.clean:
        logger.error(
            "bronze reconciliation found drift",
            extra={"orphaned": len(result.orphaned_objects), "missing": len(result.missing_objects)},
        )
        return 1

    logger.info("bronze reconciliation clean")
    return 0


def storage_stats_command(prefix: str = "bronze/") -> int:
    """Report object count and total bytes under `prefix` -- a cheap
    capacity/growth signal an operator can watch over time without needing
    a real observability platform (see the guide's Section 18, "Production
    Considerations"). Always exits 0 -- this is a reporting command, not a
    check with a pass/fail condition."""
    settings = get_settings()
    configure_logging(settings.log_level)
    s3_client = get_s3_client(settings)

    stats = get_bucket_stats(s3_client, settings.minio_bucket, prefix)
    logger.info("bronze storage stats", extra={"prefix": prefix, **stats})
    return 0


def layout_report_command(table_name: str, small_file_threshold_mb: int = 8) -> int:
    """Report per-table file-layout health: object count, average/min/max
    object size, and how many objects are "small files" under the given
    threshold (see the guide's Section 21). Always exits 0 -- reporting
    only, like `storage-stats`."""
    settings = get_settings()
    configure_logging(settings.log_level)
    s3_client = get_s3_client(settings)

    report = get_file_layout_report(
        s3_client, settings.minio_bucket, table_name,
        small_file_threshold_bytes=small_file_threshold_mb * 1024 * 1024,
    )
    logger.info("file layout report", extra={"table": table_name, **report})
    return 0


def ingestion_history_command(
    pipeline_name: str | None = None, source_table: str | None = None, limit: int = 20
) -> int:
    """Print the most recent `limit` ingestion_metadata rows, any status,
    most recent first (see the guide's Section 22). Always exits 0 --
    reporting only."""
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = engine_from_settings(settings)

    history = metadata.get_run_history(
        engine, pipeline_name=pipeline_name, source_table=source_table, limit=limit
    )
    for run in history:
        logger.info(
            "run",
            extra={
                "run_id": run["run_id"], "source_table": run["source_table"], "load_type": run["load_type"],
                "status": run["status"], "rows_written": run["rows_written"],
                "started_at": str(run["started_at"]), "error_message": run["error_message"],
            },
        )
    logger.info("ingestion history", extra={"rows_returned": len(history), "limit": limit})
    return 0


def ingestion_summary_command(config_path: Path = DEFAULT_PIPELINE_CONFIG) -> int:
    """Print one row per source table: successful/failed run counts, total
    rows ever written, and last success time (see the guide's Section
    22). Always exits 0 -- reporting only."""
    settings = get_settings()
    configure_logging(settings.log_level)

    config = _load_pipeline_config(config_path)
    pipeline_name = config["pipeline_name"]
    engine = engine_from_settings(settings)

    summary = metadata.get_ingestion_summary(engine, pipeline_name)
    for row in summary:
        logger.info(
            "table summary",
            extra={
                "source_table": row["source_table"], "successful_runs": row["successful_runs"],
                "failed_runs": row["failed_runs"], "total_rows_written": row["total_rows_written"],
                "last_success_at": str(row["last_success_at"]),
            },
        )
    return 0


def pii_report_command(contracts_dir: Path = DEFAULT_CONTRACTS_DIR) -> int:
    """Print every column any contract in `contracts_dir` classifies as
    carrying personal data, and at what sensitivity (see the guide's
    Section 23). Exits 1 if any contract is missing a `pii` declaration
    or has an invalid one -- this is a check, not just a report, since a
    column silently missing its classification is the one failure mode
    this command exists to catch."""
    settings = get_settings()
    configure_logging(settings.log_level)

    try:
        pii_columns = classify_all_contracts(contracts_dir)
    except ContractError:
        logger.exception("pii classification could not be completed")
        return 1

    if not pii_columns:
        logger.info("no columns classified as carrying personal data")
        return 0

    for col in pii_columns:
        logger.info(
            "pii column",
            extra={"table": col.table, "column": col.column, "category": col.category},
        )
    logger.info("pii report complete", extra={"columns_found": len(pii_columns)})
    return 0


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="ingest", description="url-shortener-analytics-platform ingestion CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run each configured table with its configured load_type")
    run_parser.add_argument(
        "--config", type=Path, default=DEFAULT_PIPELINE_CONFIG,
        help="Path to pipelines.yaml (default: ingestion/configs/pipelines.yaml)",
    )

    full_load_parser = subparsers.add_parser(
        "full-load", help="Force a full load of every configured table, ignoring load_type"
    )
    full_load_parser.add_argument(
        "--config", type=Path, default=DEFAULT_PIPELINE_CONFIG,
        help="Path to pipelines.yaml (default: ingestion/configs/pipelines.yaml)",
    )

    validate_contracts_parser = subparsers.add_parser(
        "validate-contracts", help="Validate contracts/source/*.yaml against the real database"
    )
    validate_contracts_parser.add_argument(
        "--contracts-dir", type=Path, default=DEFAULT_CONTRACTS_DIR,
        help="Directory of contract YAML files (default: contracts/source/)",
    )

    check_stale_runs_parser = subparsers.add_parser(
        "check-stale-runs", help="Report any ingestion_metadata row stuck in status='running'"
    )
    check_stale_runs_parser.add_argument(
        "--max-runtime-minutes", type=int, default=60,
        help="A 'running' row older than this is considered stale (default: 60)",
    )

    reconcile_bronze_parser = subparsers.add_parser(
        "reconcile-bronze", help="Compare ingestion_metadata against actual Bronze object storage"
    )
    reconcile_bronze_parser.add_argument(
        "--config", type=Path, default=DEFAULT_PIPELINE_CONFIG,
        help="Path to pipelines.yaml (default: ingestion/configs/pipelines.yaml)",
    )

    storage_stats_parser = subparsers.add_parser(
        "storage-stats", help="Report object count and total bytes under a Bronze prefix"
    )
    storage_stats_parser.add_argument(
        "--prefix", type=str, default="bronze/",
        help="Object key prefix to report stats for (default: bronze/)",
    )

    layout_report_parser = subparsers.add_parser(
        "layout-report", help="Per-table file-layout health: object count, size stats, small-file count"
    )
    layout_report_parser.add_argument("--table", type=str, required=True, help="Table name (e.g. urls, clicks)")
    layout_report_parser.add_argument(
        "--small-file-threshold-mb", type=int, default=8,
        help="Objects smaller than this are counted as 'small files' (default: 8)",
    )

    ingestion_history_parser = subparsers.add_parser(
        "ingestion-history", help="The most recent N ingestion_metadata rows, any status, most recent first"
    )
    ingestion_history_parser.add_argument("--pipeline", type=str, default=None, help="Filter by pipeline_name")
    ingestion_history_parser.add_argument("--table", type=str, default=None, help="Filter by source_table")
    ingestion_history_parser.add_argument("--limit", type=int, default=20, help="Max rows to return (default: 20)")

    ingestion_summary_parser = subparsers.add_parser(
        "ingestion-summary", help="One row per source table: run counts, total rows written, last success"
    )
    ingestion_summary_parser.add_argument(
        "--config", type=Path, default=DEFAULT_PIPELINE_CONFIG,
        help="Path to pipelines.yaml (default: ingestion/configs/pipelines.yaml)",
    )

    pii_report_parser = subparsers.add_parser(
        "pii-report", help="Report every column classified as carrying personal data, by sensitivity"
    )
    pii_report_parser.add_argument(
        "--contracts-dir", type=Path, default=DEFAULT_CONTRACTS_DIR,
        help="Directory of contract YAML files (default: contracts/source/)",
    )

    args = parser.parse_args(argv)

    if args.command == "run":
        sys.exit(run_command(args.config))
    elif args.command == "full-load":
        sys.exit(run_full_load_command(args.config))
    elif args.command == "validate-contracts":
        sys.exit(validate_contracts_command(args.contracts_dir))
    elif args.command == "check-stale-runs":
        sys.exit(check_stale_runs_command(args.max_runtime_minutes))
    elif args.command == "reconcile-bronze":
        sys.exit(reconcile_bronze_command(args.config))
    elif args.command == "storage-stats":
        sys.exit(storage_stats_command(args.prefix))
    elif args.command == "layout-report":
        sys.exit(layout_report_command(args.table, args.small_file_threshold_mb))
    elif args.command == "ingestion-history":
        sys.exit(ingestion_history_command(args.pipeline, args.table, args.limit))
    elif args.command == "ingestion-summary":
        sys.exit(ingestion_summary_command(args.config))
    elif args.command == "pii-report":
        sys.exit(pii_report_command(args.contracts_dir))


if __name__ == "__main__":
    main()
