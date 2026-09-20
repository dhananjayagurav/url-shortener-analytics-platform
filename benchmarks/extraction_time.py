"""Extraction time at scale: time this project's own `extract_full` and
`extract_incremental` functions against the real source `clicks` table, at
several row-count scales.

See docs/analytics-engineering-guide.md, Section 26 ("Performance"). This
closes the "extraction-time-at-scale benchmarks (Section 26) not yet
built" line in Section 4's Project Structure table and in Section 34's
Phase 1 completion checklist.

This calls `extract_full()` and `extract_incremental()` directly -- the
same functions `run_full_load` / `run_incremental_load` call internally --
skipping the Bronze-write step (`write_bronze` / `write_bronze_incremental`,
both of which need a real S3/MinIO client). Extraction and writing are two
separate, independently-timeable stages of the same pipeline; this script
only measures the first one, because this sandbox has no MinIO. See the
guide's Section 26.8 (Production Considerations) for why the write stage
is a known, explicitly-named open gap here, not something this script
pretends to cover.

It inserts clearly-marked SYNTHETIC rows into the real `clicks` table
(id >= SYNTHETIC_ID_FLOOR) purely to get real extraction-timing numbers at
scale, then DELETES every row it inserted before exiting, by default,
leaving `clicks` exactly as this script found it (5,003 real rows, id
1-5003, as of Section 26). Pass --keep-data to skip cleanup.

Needs only real Postgres -- no MinIO required.

Run:
    PYTHONPATH=ingestion/src python3 benchmarks/extraction_time.py
    PYTHONPATH=ingestion/src python3 benchmarks/extraction_time.py --scales 5000,50000,500000
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion" / "src"))

from sqlalchemy import Engine, text  # noqa: E402

from url_shortener_analytics.config import get_settings  # noqa: E402
from url_shortener_analytics.db import engine_from_settings  # noqa: E402
from url_shortener_analytics.extract_full import extract_full  # noqa: E402
from url_shortener_analytics.extract_incremental import extract_incremental  # noqa: E402

# Well clear of the real `clicks` table's contiguous ids (1-5003 as of
# Section 26 -- see Section 26.2's real row-count check).
SYNTHETIC_ID_FLOOR = 10_000_000

DEVICE_TYPES = ["mobile", "desktop", "tablet", "unknown"]


def cleanup(engine: Engine) -> None:
    """Delete every synthetic row this script could ever have inserted.
    Called both before (defensive, in case a prior run crashed and left
    rows behind -- the same lesson Section 22 and query_performance.py
    already apply) and after a normal run."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM clicks WHERE id >= :f"), {"f": SYNTHETIC_ID_FLOOR})


def _real_row_count(engine: Engine) -> int:
    with engine.begin() as conn:
        return conn.execute(text("SELECT count(*) FROM clicks")).scalar_one()


def _generate_synthetic_clicks(engine: Engine, n_rows: int) -> None:
    """Bulk-insert n_rows synthetic clicks rows, ids
    SYNTHETIC_ID_FLOOR..SYNTHETIC_ID_FLOOR+n_rows-1, short_code/user_id
    reused from the real urls/users tables so the row "looks like" a real
    click, even though extraction timing does not depend on that."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO clicks (id, short_code, occurred_at, device_type, hashed_ip, user_id)
                SELECT
                    :floor + g,
                    (SELECT short_code FROM urls ORDER BY random() LIMIT 1),
                    now(),
                    (ARRAY['mobile','desktop','tablet','unknown'])[1 + (g % 4)],
                    md5(('synthetic-ip-' || g)::text),
                    CASE WHEN g % 5 = 0 THEN NULL ELSE (SELECT id FROM users ORDER BY random() LIMIT 1) END
                FROM generate_series(0, :n_rows - 1) AS g
                """
            ),
            {"floor": SYNTHETIC_ID_FLOOR, "n_rows": n_rows},
        )


def time_extract_full(engine: Engine, n_repeats: int = 3) -> float:
    """Best-of-n_repeats wall-clock seconds for a full-table extract --
    same best-of-3 noise-reduction reasoning query_performance.py and
    Section 19.6 already used."""
    samples = []
    for _ in range(n_repeats):
        start = time.perf_counter()
        extract_full("clicks", engine)
        samples.append(time.perf_counter() - start)
    return min(samples)


def time_extract_incremental(engine: Engine, watermark: int, n_repeats: int = 3) -> float:
    """Best-of-n_repeats wall-clock seconds to read only the rows with
    id > watermark -- the realistic incremental-load shape, where the
    watermark already sits at the end of the previous batch."""
    samples = []
    for _ in range(n_repeats):
        start = time.perf_counter()
        extract_incremental("clicks", engine, watermark=watermark)
        samples.append(time.perf_counter() - start)
    return min(samples)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scales", type=str, default="5000,50000,500000", help="Comma-separated synthetic row counts to add on top of the real clicks rows")
    parser.add_argument("--keep-data", action="store_true", help="Skip cleanup; leave the last scale's synthetic rows in clicks for manual inspection")
    args = parser.parse_args()
    scales = [int(s) for s in args.scales.split(",")]

    settings = get_settings()
    engine = engine_from_settings(settings)

    real_rows = _real_row_count(engine)
    print(f"Real (non-synthetic) clicks rows found: {real_rows:,}")

    cleanup(engine)  # in case a prior failed run left rows behind

    all_results: list[dict[str, object]] = []
    for scale in scales:
        cleanup(engine)
        gen_start = time.perf_counter()
        _generate_synthetic_clicks(engine, scale)
        gen_s = time.perf_counter() - gen_start
        total_rows = real_rows + scale
        print(f"\n--- {total_rows:,} total clicks rows ({real_rows:,} real + {scale:,} synthetic) ---")
        print(f"  (synthetic insert took {gen_s:.2f}s -- not part of the timed result)")

        full_s = time_extract_full(engine)
        # Watermark set to the row just below the synthetic batch, so the
        # incremental extract reads exactly this batch's new rows -- the
        # realistic case where the previous run's watermark already
        # covers everything older.
        incr_s = time_extract_incremental(engine, watermark=SYNTHETIC_ID_FLOOR - 1)

        print(f"  extract_full          {full_s * 1000:>9.2f} ms  (reads all {total_rows:,} rows)")
        print(f"  extract_incremental   {incr_s * 1000:>9.2f} ms  (reads only the {scale:,} new rows)")

        all_results.append({"total_rows": total_rows, "synthetic_rows": scale, "extract_full_ms": round(full_s * 1000, 2)})
        all_results.append({"total_rows": total_rows, "synthetic_rows": scale, "extract_incremental_ms": round(incr_s * 1000, 2)})

    if args.keep_data:
        print(f"\n--keep-data set: {scales[-1]:,} synthetic rows left in clicks for manual inspection.")
    else:
        cleanup(engine)
        final_rows = _real_row_count(engine)
        print(f"\nCleaned up -- clicks restored to {final_rows:,} rows (expected {real_rows:,}).")

    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(exist_ok=True)
    out_path = results_dir / "extraction_time.json"
    out_path.write_text(json.dumps(all_results, indent=2))
    print(f"\n(results also written to {out_path}, gitignored, for the reader's own reference)")


if __name__ == "__main__":
    main()
