from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import Engine, text

from url_shortener_analytics import metadata


def test_watermark_defaults_to_zero_for_a_never_run_table(sqlite_engine: Engine) -> None:
    assert metadata.get_last_watermark(sqlite_engine, "pipeline_a", "clicks") == 0


def test_successful_run_advances_the_watermark(sqlite_engine: Engine) -> None:
    run_id = metadata.start_run(sqlite_engine, "pipeline_a", "clicks", load_type="incremental")
    metadata.finish_run_success(sqlite_engine, run_id, rows_read=10, rows_written=10, watermark_end=42)

    assert metadata.get_last_watermark(sqlite_engine, "pipeline_a", "clicks") == 42


def test_failed_run_does_not_move_the_watermark(sqlite_engine: Engine) -> None:
    run_id_1 = metadata.start_run(sqlite_engine, "pipeline_a", "clicks", load_type="incremental")
    metadata.finish_run_success(sqlite_engine, run_id_1, rows_read=10, rows_written=10, watermark_end=42)

    run_id_2 = metadata.start_run(sqlite_engine, "pipeline_a", "clicks", load_type="incremental")
    metadata.finish_run_failure(sqlite_engine, run_id_2, "simulated failure")

    # The failed run must not become the new watermark source -- the last
    # *successful* run (42) is still what the next run should resume from.
    assert metadata.get_last_watermark(sqlite_engine, "pipeline_a", "clicks") == 42


def test_watermark_is_scoped_per_pipeline_and_table(sqlite_engine: Engine) -> None:
    run_id = metadata.start_run(sqlite_engine, "pipeline_a", "clicks", load_type="incremental")
    metadata.finish_run_success(sqlite_engine, run_id, rows_read=1, rows_written=1, watermark_end=99)

    # A different table under the same pipeline has its own, independent watermark.
    assert metadata.get_last_watermark(sqlite_engine, "pipeline_a", "urls") == 0
    # A different pipeline name is also independent, even for the same table.
    assert metadata.get_last_watermark(sqlite_engine, "pipeline_b", "clicks") == 0


def test_finish_run_success_persists_the_bronze_key(sqlite_engine: Engine) -> None:
    run_id = metadata.start_run(sqlite_engine, "pipeline_a", "clicks", load_type="full")
    metadata.finish_run_success(sqlite_engine, run_id, rows_read=5, rows_written=5, bronze_key="bronze/clicks/x.parquet")

    with sqlite_engine.begin() as conn:
        row = conn.execute(
            text("SELECT bronze_key FROM ingestion_metadata WHERE run_id = :run_id"), {"run_id": run_id}
        ).fetchone()
    assert row.bronze_key == "bronze/clicks/x.parquet"


def test_finish_run_success_defaults_bronze_key_to_none(sqlite_engine: Engine) -> None:
    # The no-op incremental path (no new rows) calls finish_run_success
    # without a bronze_key -- see extract_incremental.run_incremental_load.
    run_id = metadata.start_run(sqlite_engine, "pipeline_a", "clicks", load_type="incremental")
    metadata.finish_run_success(sqlite_engine, run_id, rows_read=0, rows_written=0)

    with sqlite_engine.begin() as conn:
        row = conn.execute(
            text("SELECT bronze_key FROM ingestion_metadata WHERE run_id = :run_id"), {"run_id": run_id}
        ).fetchone()
    assert row.bronze_key is None


def _set_started_at(engine: Engine, run_id: str, started_at: datetime) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE ingestion_metadata SET started_at = :started_at WHERE run_id = :run_id"),
            {"started_at": started_at, "run_id": run_id},
        )


def test_find_stale_running_runs_finds_a_run_older_than_the_cutoff(sqlite_engine: Engine) -> None:
    run_id = metadata.start_run(sqlite_engine, "pipeline_a", "clicks", load_type="incremental")
    _set_started_at(sqlite_engine, run_id, datetime.now(UTC) - timedelta(minutes=120))

    stale = metadata.find_stale_running_runs(sqlite_engine, max_runtime_minutes=60)

    assert len(stale) == 1
    assert stale[0]["run_id"] == run_id
    assert stale[0]["source_table"] == "clicks"


def test_find_stale_running_runs_excludes_a_run_within_the_cutoff(sqlite_engine: Engine) -> None:
    run_id = metadata.start_run(sqlite_engine, "pipeline_a", "clicks", load_type="incremental")
    _set_started_at(sqlite_engine, run_id, datetime.now(UTC) - timedelta(minutes=10))

    assert metadata.find_stale_running_runs(sqlite_engine, max_runtime_minutes=60) == []


def test_find_stale_running_runs_excludes_success_and_failed_rows(sqlite_engine: Engine) -> None:
    old = datetime.now(UTC) - timedelta(minutes=120)

    success_run = metadata.start_run(sqlite_engine, "pipeline_a", "clicks", load_type="incremental")
    _set_started_at(sqlite_engine, success_run, old)
    metadata.finish_run_success(sqlite_engine, success_run, rows_read=1, rows_written=1, watermark_end=1)

    failed_run = metadata.start_run(sqlite_engine, "pipeline_a", "urls", load_type="full")
    _set_started_at(sqlite_engine, failed_run, old)
    metadata.finish_run_failure(sqlite_engine, failed_run, "simulated failure")

    # Neither a completed success nor a completed failure is "stale" -- only
    # a row still stuck in status='running' is a crashed-run signal.
    assert metadata.find_stale_running_runs(sqlite_engine, max_runtime_minutes=60) == []


def test_find_stale_running_runs_can_be_filtered_by_pipeline_name(sqlite_engine: Engine) -> None:
    old = datetime.now(UTC) - timedelta(minutes=120)

    run_a = metadata.start_run(sqlite_engine, "pipeline_a", "clicks", load_type="incremental")
    _set_started_at(sqlite_engine, run_a, old)
    run_b = metadata.start_run(sqlite_engine, "pipeline_b", "clicks", load_type="incremental")
    _set_started_at(sqlite_engine, run_b, old)

    stale = metadata.find_stale_running_runs(sqlite_engine, max_runtime_minutes=60, pipeline_name="pipeline_a")

    assert [r["run_id"] for r in stale] == [run_a]


def test_list_successful_bronze_keys_excludes_failed_runs_and_null_keys(sqlite_engine: Engine) -> None:
    run_1 = metadata.start_run(sqlite_engine, "pipeline_a", "clicks", load_type="full")
    metadata.finish_run_success(sqlite_engine, run_1, rows_read=1, rows_written=1, bronze_key="bronze/clicks/a.parquet")

    # No-op incremental run: success, but no bronze_key.
    run_2 = metadata.start_run(sqlite_engine, "pipeline_a", "clicks", load_type="incremental")
    metadata.finish_run_success(sqlite_engine, run_2, rows_read=0, rows_written=0)

    run_3 = metadata.start_run(sqlite_engine, "pipeline_a", "urls", load_type="full")
    metadata.finish_run_failure(sqlite_engine, run_3, "simulated failure")

    assert metadata.list_successful_bronze_keys(sqlite_engine) == ["bronze/clicks/a.parquet"]


def test_list_successful_bronze_keys_can_be_filtered_by_pipeline_name(sqlite_engine: Engine) -> None:
    run_a = metadata.start_run(sqlite_engine, "pipeline_a", "clicks", load_type="full")
    metadata.finish_run_success(sqlite_engine, run_a, rows_read=1, rows_written=1, bronze_key="bronze/a.parquet")

    run_b = metadata.start_run(sqlite_engine, "pipeline_b", "clicks", load_type="full")
    metadata.finish_run_success(sqlite_engine, run_b, rows_read=1, rows_written=1, bronze_key="bronze/b.parquet")

    assert metadata.list_successful_bronze_keys(sqlite_engine, pipeline_name="pipeline_a") == ["bronze/a.parquet"]
