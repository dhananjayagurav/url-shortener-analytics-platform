"""Command-line entrypoint: `python -m url_shortener_analytics.cli <command>`
(or the `ingest` console script installed by pyproject.toml).

Two commands:

- `run`        -- dispatch each table in pipelines.yaml to full or
                  incremental load, per its configured `load_type`. This is
                  what a scheduled/production run should call.
- `full-load`  -- force a full load of every configured table, regardless
                  of its configured `load_type`. Kept as an explicit
                  operator override for backfills/rebuilds -- see the
                  guide's Section 15, "Production Considerations", for when
                  you'd reach for this over `run`.

See docs/analytics-engineering-guide.md, "Full Load Ingestion -> How to
Run" and Section 15 "How to Run", for the exact commands and expected
output.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import yaml

from url_shortener_analytics.config import get_settings
from url_shortener_analytics.db import engine_from_settings
from url_shortener_analytics.extract_full import run_full_load
from url_shortener_analytics.extract_incremental import run_incremental_load
from url_shortener_analytics.logging_setup import configure_logging
from url_shortener_analytics.object_store import get_s3_client

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

    args = parser.parse_args(argv)

    if args.command == "run":
        sys.exit(run_command(args.config))
    elif args.command == "full-load":
        sys.exit(run_full_load_command(args.config))


if __name__ == "__main__":
    main()
