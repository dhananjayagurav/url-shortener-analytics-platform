"""Write a real Bronze `clicks` Parquet snapshot to the local filesystem.

Why this script exists (read before assuming it's dead code):

`ingestion/src/url_shortener_analytics/object_store.py`'s `write_bronze`
writes to a real S3-compatible bucket (MinIO in Docker Compose, a real
bucket in production). This sandbox has neither Docker-with-a-running-
daemon nor network access to download a standalone MinIO binary (both were
tried and confirmed unavailable before writing this script -- see
docs/analytics-engineering-guide.md, Phase 2, ADR-016). So the *first*
Spark component -- Bronze `clicks` -> Silver `clicks`, Phase 2's opening
milestone -- has no real Bronze object to read from in this environment.

This script closes that gap honestly: it calls the real, unmodified
`extract_full("clicks", engine)` against the real, running Postgres
instance (the same function `run_full_load` calls), and reuses the real
`build_bronze_key` function so the local file lands at the exact same
relative key a real MinIO object would use -- just rooted at `data/`
instead of a bucket. Nothing about the *data* is synthetic; only the
*storage backend* is substituted, and only because this sandbox cannot run
the real one. A real deployment (or a laptop with Docker) runs
`make ingest` / `make ingest-full` instead and never needs this script.

One-off operator script, not part of either the `url_shortener_analytics`
or `analytics_transform` package -- same convention as
`scripts/seed_sample_data.py`.
"""

from __future__ import annotations

import argparse
import io
import logging
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from url_shortener_analytics.config import get_settings
from url_shortener_analytics.db import engine_from_settings
from url_shortener_analytics.extract_full import extract_full
from url_shortener_analytics.logging_setup import configure_logging
from url_shortener_analytics.object_store import build_bronze_key

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"


def write_local_bronze_clicks(data_root: Path = DEFAULT_DATA_ROOT) -> Path:
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = engine_from_settings(settings)

    df = extract_full("clicks", engine)

    run_date = datetime.now(UTC)
    key = build_bronze_key("clicks", run_date)  # bronze/clicks/ingestion_date=YYYY-MM-DD/clicks.parquet
    out_path = data_root / key
    out_path.parent.mkdir(parents=True, exist_ok=True)

    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buffer, compression="snappy")
    out_path.write_bytes(buffer.getvalue())

    logger.info(
        "wrote local bronze clicks snapshot",
        extra={"path": str(out_path), "rows": len(df), "bytes": out_path.stat().st_size},
    )
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root", type=Path, default=DEFAULT_DATA_ROOT,
        help=f"Local data lake root (default: {DEFAULT_DATA_ROOT})",
    )
    args = parser.parse_args()
    path = write_local_bronze_clicks(args.data_root)
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
