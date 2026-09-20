"""Unit tests for list_local_bronze_files -- no Spark needed, this is
pure filesystem logic (pytest's tmp_path fixture, not a real repo path)."""

from __future__ import annotations

from analytics_transform.config import list_local_bronze_files


def test_finds_both_full_load_and_incremental_files(tmp_path) -> None:
    clicks_root = tmp_path / "clicks"
    full_load = clicks_root / "ingestion_date=2026-09-20" / "clicks.parquet"
    incremental = (
        clicks_root / "incremental" / "watermark_start=000000005003"
        / "watermark_end=000000005203" / "clicks.parquet"
    )
    full_load.parent.mkdir(parents=True)
    full_load.write_bytes(b"not-real-parquet-bytes-just-testing-discovery")
    incremental.parent.mkdir(parents=True)
    incremental.write_bytes(b"not-real-parquet-bytes-just-testing-discovery")

    found = list_local_bronze_files(tmp_path, "clicks")

    assert found == sorted([full_load, incremental])


def test_returns_empty_list_when_table_has_no_bronze_files(tmp_path) -> None:
    assert list_local_bronze_files(tmp_path, "clicks") == []


def test_ignores_other_tables(tmp_path) -> None:
    urls_file = tmp_path / "urls" / "ingestion_date=2026-09-20" / "urls.parquet"
    urls_file.parent.mkdir(parents=True)
    urls_file.write_bytes(b"not-real-parquet-bytes")

    assert list_local_bronze_files(tmp_path, "clicks") == []
