"""Parquet vs. CSV vs. JSON-lines: file size, write time, full-read time, and
single-column-read time, on this project's own real (seeded) `clicks` table.

See docs/analytics-engineering-guide.md, Section 19 ("Parquet"), for the
concept and the full discussion of these results. This is the benchmark
ADR-003 named as "planned" -- this script, and the numbers it prints, are
what closes that.

Per this directory's own README and the guide's rule on fabricated results:
nothing this script produces is written into the guide without this exact
script having actually been run first. Every number in Section 19 is
labeled ACTUAL OBSERVED and came from a real run of this file.

Run:
    PYTHONPATH=ingestion/src python3 benchmarks/parquet_vs_csv_vs_json.py [--rows N]

With no --rows, reads the real `clicks` table as-is (whatever `make seed`
put there -- 5,000 rows by default). --rows N ignores the database
entirely and generates N synthetic rows with the same shape instead, to
show whether the same trends hold at a larger, more realistic scale than
this repo's small seeded dataset -- clearly a *different*, synthetic
dataset, never blended with or presented as the real one.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion" / "src"))

from url_shortener_analytics.config import get_settings  # noqa: E402
from url_shortener_analytics.db import engine_from_settings  # noqa: E402

FORMATS = ("csv", "json", "parquet")


def _load_real_clicks() -> pd.DataFrame:
    settings = get_settings()
    engine = engine_from_settings(settings)
    return pd.read_sql_table("clicks", engine)


def _generate_synthetic_clicks(n_rows: int) -> pd.DataFrame:
    """Same column shape as the real `clicks` table, generated in pure
    Python -- no database round-trip, so this scales to a much larger row
    count than this repo's seeded 5,000 rows without needing a bigger
    Postgres instance. Explicitly synthetic; never presented as real data."""
    random.seed(42)
    device_types = ["mobile", "desktop", "tablet"]
    return pd.DataFrame(
        {
            "id": range(1, n_rows + 1),
            "short_code": [f"abc{i % 500:03d}" for i in range(n_rows)],
            "occurred_at": pd.date_range("2026-01-01", periods=n_rows, freq="s"),
            "device_type": [random.choices(device_types, weights=[60, 35, 5])[0] for _ in range(n_rows)],
            "hashed_ip": [f"{i:064x}" for i in range(n_rows)],
            "user_id": [random.randint(1, 200) if random.random() < 0.3 else None for _ in range(n_rows)],
        }
    )


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)


def _write_json(df: pd.DataFrame, path: Path) -> None:
    df.to_json(path, orient="records", lines=True, date_format="iso")


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    df.to_parquet(path, engine="pyarrow", compression="snappy", index=False)


WRITERS = {"csv": _write_csv, "json": _write_json, "parquet": _write_parquet}


def _read_csv_full(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _read_json_full(path: Path) -> pd.DataFrame:
    return pd.read_json(path, orient="records", lines=True)


def _read_parquet_full(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path, engine="pyarrow")


READERS_FULL = {"csv": _read_csv_full, "json": _read_json_full, "parquet": _read_parquet_full}


def _read_csv_one_column(path: Path) -> pd.DataFrame:
    # usecols still has to scan the file, but skips building columns it
    # doesn't need -- the fairest CSV can do; it is NOT true columnar I/O.
    return pd.read_csv(path, usecols=["device_type"])


def _read_json_one_column(path: Path) -> pd.DataFrame:
    # JSON-lines has no columnar shortcut at all: every line must be fully
    # parsed before any column can be selected out of it.
    return pd.read_json(path, orient="records", lines=True)[["device_type"]]


def _read_parquet_one_column(path: Path) -> pd.DataFrame:
    # True columnar pushdown: pyarrow reads only the device_type column's
    # bytes off disk, never touching the other five columns' data at all.
    return pd.read_parquet(path, engine="pyarrow", columns=["device_type"])


READERS_ONE_COLUMN = {
    "csv": _read_csv_one_column, "json": _read_json_one_column, "parquet": _read_parquet_one_column,
}

EXTENSIONS = {"csv": "csv", "json": "jsonl", "parquet": "parquet"}


def run_benchmark(df: pd.DataFrame, label: str) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    with TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        for fmt in FORMATS:
            path = tmp / f"clicks.{EXTENSIONS[fmt]}"

            t0 = time.perf_counter()
            WRITERS[fmt](df, path)
            write_s = time.perf_counter() - t0

            size_bytes = path.stat().st_size

            t0 = time.perf_counter()
            READERS_FULL[fmt](path)
            full_read_s = time.perf_counter() - t0

            t0 = time.perf_counter()
            READERS_ONE_COLUMN[fmt](path)
            one_col_read_s = time.perf_counter() - t0

            results.append(
                {
                    "dataset": label,
                    "format": fmt,
                    "rows": len(df),
                    "size_bytes": size_bytes,
                    "write_s": round(write_s, 4),
                    "full_read_s": round(full_read_s, 4),
                    "one_column_read_s": round(one_col_read_s, 4),
                }
            )
    return results


def print_results(results: list[dict[str, object]]) -> None:
    header = f"{'dataset':<20} {'format':<8} {'rows':>7} {'size_bytes':>12} {'write_s':>9} {'full_read_s':>12} {'one_col_read_s':>15}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['dataset']:<20} {r['format']:<8} {r['rows']:>7} {r['size_bytes']:>12} "
            f"{r['write_s']:>9} {r['full_read_s']:>12} {r['one_column_read_s']:>15}"
        )

    # Size and full-read comparisons, relative to Parquet, per dataset.
    by_dataset: dict[str, dict[str, dict[str, object]]] = {}
    for r in results:
        by_dataset.setdefault(r["dataset"], {})[r["format"]] = r
    print()
    for dataset, by_fmt in by_dataset.items():
        parquet_size = by_fmt["parquet"]["size_bytes"]
        for fmt in ("csv", "json"):
            ratio = by_fmt[fmt]["size_bytes"] / parquet_size
            print(f"{dataset}: {fmt} is {ratio:.2f}x the size of parquet")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=None, help="Generate N synthetic rows instead of reading the real clicks table")
    args = parser.parse_args()

    all_results: list[dict[str, object]] = []

    if args.rows is None:
        df_real = _load_real_clicks()
        print(f"Loaded {len(df_real)} REAL rows from this project's own `clicks` table.\n")
        all_results.extend(run_benchmark(df_real, "real_clicks"))
    else:
        df_synthetic = _generate_synthetic_clicks(args.rows)
        print(f"Generated {len(df_synthetic)} SYNTHETIC rows (same shape as `clicks`, not real data).\n")
        all_results.extend(run_benchmark(df_synthetic, f"synthetic_{args.rows}"))

    print_results(all_results)

    # Not committed -- see benchmarks/README.md.
    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(exist_ok=True)
    out_path = results_dir / f"parquet_vs_csv_vs_json_{all_results[0]['dataset']}.json"
    out_path.write_text(json.dumps(all_results, indent=2))
    print(f"\n(results also written to {out_path}, gitignored, for the reader's own reference)")


if __name__ == "__main__":
    main()
