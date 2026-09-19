"""Command-line entrypoint: `python -m url_shortener_analytics.cli full-load`
(or the `ingest` console script installed by pyproject.toml).

See docs/analytics-engineering-guide.md, "Full Load Ingestion -> How to
Run", for the exact commands and expected output.
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
from url_shortener_analytics.logging_setup import configure_logging
from url_shortener_analytics.object_store import get_s3_client

logger = logging.getLogger(__name__)

DEFAULT_PIPELINE_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "pipelines.yaml"


def _load_pipeline_config(path: Path) -> dict:
    with path.open() as f:
        return yaml.safe_load(f)


def run_full_load_command(config_path: Path = DEFAULT_PIPELINE_CONFIG) -> int:
    settings = get_settings()
    configure_logging(settings.log_level)

    config = _load_pipeline_config(config_path)
    pipeline_name = config["pipeline_name"]
    tables = [t["name"] for t in config["tables"]]

    engine = engine_from_settings(settings)
    s3_client = get_s3_client(settings)

    logger.info("starting full load", extra={"pipeline": pipeline_name, "tables": tables})

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


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="ingest", description="url-shortener-analytics-platform ingestion CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    full_load_parser = subparsers.add_parser("full-load", help="Run a full load for every configured table")
    full_load_parser.add_argument(
        "--config", type=Path, default=DEFAULT_PIPELINE_CONFIG,
        help="Path to pipelines.yaml (default: ingestion/configs/pipelines.yaml)",
    )

    args = parser.parse_args(argv)

    if args.command == "full-load":
        sys.exit(run_full_load_command(args.config))


if __name__ == "__main__":
    main()
