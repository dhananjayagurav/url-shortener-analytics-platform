from __future__ import annotations

from jobs.explore_clicks import build_valid_clicks


def test_build_valid_clicks_drops_null_ids_and_lowercases_device_type(local_spark) -> None:
    raw = local_spark.createDataFrame(
        [
            (1, "abc123", "2026-01-01T00:00:00", "MOBILE"),
            (None, "def456", "2026-01-01T00:05:00", "Desktop"),
            (3, "ghi789", "2026-01-01T00:10:00", "tablet"),
        ],
        ["id", "short_code", "occurred_at", "device_type"],
    )

    result = build_valid_clicks(raw).collect()

    assert len(result) == 2
    assert {row.id for row in result} == {1, 3}
    assert {row.device_type for row in result} == {"mobile", "tablet"}