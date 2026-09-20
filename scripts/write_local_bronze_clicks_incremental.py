"""Write a real, INCREMENTAL Bronze `clicks` Parquet batch to the local
filesystem -- the second Bronze object type Section 39 needs, alongside
the full-load snapshot `write_local_bronze_clicks.py` already produces.

Same reasoning as `write_local_bronze_clicks.py`'s own docstring (read
that first): this sandbox has no reachable MinIO/S3 (ADR-016), so this
calls the real, unmodified `extract_incremental` against the real,
running Postgres, and reuses the real `build_bronze_incremental_key`, just
rooted at `data/` instead of a bucket.

One thing this script does NOT do, deliberately: read or write
`ingestion_metadata` at all. `run_incremental_load` (the real, production
orchestration function -- ingestion/src/url_shortener_analytics/extract_incremental.py)
normally gets its watermark from the last successful checkpoint there.
This script isn't standing in for that orchestration -- Bronze full-load
checkpointing was never recorded either, since `write_local_bronze_clicks.py`
bypasses it the same way, for the same reason (no real MinIO to make a
"real" checkpointed run meaningful against in this environment). Instead,
the watermark this script uses is read directly from the full-load Bronze
snapshot already on disk (its real, actual max `id`) -- so "what's the
watermark" has one honest, traceable answer: whatever Bronze itself
already contains, not an assumed or hardcoded number.

Run:
    python scripts/write_local_bronze_clicks_incremental.py
"""

from __future__ import annotations

import argparse
import io
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from url_shortener_analytics.config import get_settings
from url_shortener_analytics.db import engine_from_settings
from url_shortener_analytics.extract_incremental import extract_incremental
from url_shortener_analytics.logging_setup import configure_logging
from url_shortener_analytics.object_store import build_bronze_incremental_key

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"


def _watermark_from_existing_bronze(data_root: Path) -> int:
    """The real max `id` already captured in the full-load Bronze
    snapshot on disk -- this is what makes the watermark this script uses
    traceable to something real, rather than a guessed or hardcoded
    number. Raises if no full-load snapshot exists yet -- there is
    nothing honest to compute a watermark from otherwise."""
    full_load_files = sorted((data_root / "bronze" / "clicks").glob("ingestion_date=*/clicks.parquet"))
    if not full_load_files:
        raise FileNotFoundError(
            "no full-load Bronze clicks snapshot found under "
            f"{data_root / 'bronze' / 'clicks'} -- run "
            "`make write-local-bronze-clicks` first."
        )
    # If more than one full-load snapshot exists (re-run on a later day),
    # the most recent one is the honest baseline to extract past.
    latest = full_load_files[-1]
    ids = pq.read_table(latest, columns=["id"]).column("id").to_pylist()
    if not ids:
        raise ValueError(f"full-load bronze snapshot at {latest} has no rows to derive a watermark from")
    return max(ids)


def write_local_bronze_clicks_incremental(data_root: Path = DEFAULT_DATA_ROOT) -> Path | None:
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = engine_from_settings(settings)

    watermark = _watermark_from_existing_bronze(data_root)
    logger.info("watermark read from existing full-load bronze snapshot", extra={"watermark": watermark})

    df = extract_incremental("clicks", engine, watermark)

    if df.empty:
        logger.info("no new rows since watermark, nothing to write", extra={"watermark": watermark})
        print(f"no new rows since watermark={watermark}")
        return None

    new_watermark = int(df["id"].max())
    key = build_bronze_incremental_key("clicks", watermark, new_watermark)
    out_path = data_root / key
    out_path.parent.mkdir(parents=True, exist_ok=True)

    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buffer, compression="snappy")
    out_path.write_bytes(buffer.getvalue())

    logger.info(
        "wrote local incremental bronze clicks batch",
        extra={
            "path": str(out_path), "rows": len(df), "bytes": out_path.stat().st_size,
            "watermark_start": watermark, "watermark_end": new_watermark,
        },
    )
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root", type=Path, default=DEFAULT_DATA_ROOT,
        help=f"Local data lake root (default: {DEFAULT_DATA_ROOT})",
    )
    args = parser.parse_args()
    path = write_local_bronze_clicks_incremental(args.data_root)
    print(f"wrote {path}" if path else "wrote nothing (no new rows)")


if __name__ == "__main__":
    main()
