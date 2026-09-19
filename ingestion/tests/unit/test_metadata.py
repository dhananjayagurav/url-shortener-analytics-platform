from __future__ import annotations

from sqlalchemy import Engine

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
