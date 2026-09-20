"""Unit tests for clean_clicks -- built on deliberately dirty rows, not
this repo's real (already-clean) seeded data. See object_store.py-style
reasoning in scripts/write_local_bronze_clicks.py's docstring for why the
*real* Bronze run (Section 38's "How to Run") doesn't, on its own, prove
this logic works: the seeded `clicks` table satisfies its own contract
already, so nothing in it exercises the coercion/drop paths below. These
seven rows do, the same way ingestion's own unit tests use synthetic edge
cases (not the real seeded database) to prove watermark/checkpoint logic.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from analytics_transform.silver.transform_clicks import clean_clicks, read_bronze_clicks
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Matches what pyarrow/pandas actually produced when Bronze clicks was
# written for real (verified by inspecting the real Parquet file's schema
# before writing this test) -- notably `user_id` as DoubleType, not
# LongType: pandas represents a nullable integer column with NaNs as
# float64, so that's the physical type Bronze really has. This is exactly
# the quirk clean_clicks's schema-normalization step exists to fix.
BRONZE_CLICKS_SCHEMA = StructType(
    [
        StructField("id", LongType(), nullable=True),
        StructField("short_code", StringType(), nullable=True),
        StructField("occurred_at", TimestampType(), nullable=True),
        StructField("device_type", StringType(), nullable=True),
        StructField("hashed_ip", StringType(), nullable=True),
        StructField("user_id", DoubleType(), nullable=True),
    ]
)

VALID_HASH = "a" * 64
TS = datetime(2026, 9, 20, 12, 0, 0, tzinfo=UTC)


def _bronze_df(spark: SparkSession):
    rows = [
        # 1: clean row, every field already valid.
        Row(id=1, short_code="abc123", occurred_at=TS, device_type="mobile", hashed_ip=VALID_HASH, user_id=42.0),
        # 2: recoverable -- whitespace, mixed-case device_type, malformed hash, null user_id.
        Row(id=2, short_code="  xyz789  ", occurred_at=TS, device_type=" Mobile ", hashed_ip="not-a-hash", user_id=None),
        # 3: unrecognized device_type -- coerced to 'unknown', not dropped.
        Row(id=3, short_code="qqq111", occurred_at=TS, device_type="smart-tv", hashed_ip=None, user_id=7.0),
        # 4: null click_id (id) -- not recoverable, dropped.
        Row(id=None, short_code="drop-me", occurred_at=TS, device_type="mobile", hashed_ip=None, user_id=None),
        # 5: null occurred_at -- not recoverable, dropped.
        Row(id=5, short_code="drop-me-too", occurred_at=None, device_type="mobile", hashed_ip=None, user_id=None),
        # 6: empty short_code -- not recoverable, dropped.
        Row(id=6, short_code="", occurred_at=TS, device_type="mobile", hashed_ip=None, user_id=None),
        # 7: whitespace-only short_code -- empty after trim, dropped.
        Row(id=7, short_code="   ", occurred_at=TS, device_type="mobile", hashed_ip=None, user_id=None),
    ]
    return spark.createDataFrame(rows, schema=BRONZE_CLICKS_SCHEMA)


def test_drops_rows_missing_required_fields(spark: SparkSession) -> None:
    silver_df = clean_clicks(_bronze_df(spark))
    # Rows 4, 5, 6, 7 are unrecoverable; rows 1, 2, 3 survive.
    assert silver_df.count() == 3
    surviving_ids = {row.click_id for row in silver_df.collect()}
    assert surviving_ids == {1, 2, 3}


def test_renames_id_to_click_id_and_casts_to_long(spark: SparkSession) -> None:
    silver_df = clean_clicks(_bronze_df(spark))
    assert "id" not in silver_df.columns
    assert "click_id" in silver_df.columns
    assert dict(silver_df.dtypes)["click_id"] == "bigint"


def test_trims_short_code(spark: SparkSession) -> None:
    silver_df = clean_clicks(_bronze_df(spark))
    row = silver_df.filter("click_id = 2").first()
    assert row.short_code == "xyz789"


def test_normalizes_device_type_case_and_whitespace(spark: SparkSession) -> None:
    silver_df = clean_clicks(_bronze_df(spark))
    row = silver_df.filter("click_id = 2").first()
    assert row.device_type == "mobile"


def test_coerces_unrecognized_device_type_to_unknown(spark: SparkSession) -> None:
    silver_df = clean_clicks(_bronze_df(spark))
    row = silver_df.filter("click_id = 3").first()
    assert row.device_type == "unknown"


def test_nulls_malformed_hashed_ip_without_dropping_the_row(spark: SparkSession) -> None:
    silver_df = clean_clicks(_bronze_df(spark))
    row = silver_df.filter("click_id = 2").first()
    assert row.hashed_ip is None


def test_preserves_valid_hashed_ip(spark: SparkSession) -> None:
    silver_df = clean_clicks(_bronze_df(spark))
    row = silver_df.filter("click_id = 1").first()
    assert row.hashed_ip == VALID_HASH


def test_casts_user_id_to_long_and_preserves_null(spark: SparkSession) -> None:
    silver_df = clean_clicks(_bronze_df(spark))
    assert dict(silver_df.dtypes)["user_id"] == "bigint"
    row1 = silver_df.filter("click_id = 1").first()
    row2 = silver_df.filter("click_id = 2").first()
    assert row1.user_id == 42
    assert row2.user_id is None


def test_occurred_at_is_timestamp_type(spark: SparkSession) -> None:
    silver_df = clean_clicks(_bronze_df(spark))
    assert dict(silver_df.dtypes)["occurred_at"] == "timestamp"


def test_adds_silver_loaded_at_column(spark: SparkSession) -> None:
    silver_df = clean_clicks(_bronze_df(spark))
    assert "silver_loaded_at" in silver_df.columns
    assert all(row.silver_loaded_at is not None for row in silver_df.collect())


# --- read_bronze_clicks: real files on disk, both Bronze partitioning
# schemes at once. This is the real regression test for Section 39's
# fix -- before it, spark.read.parquet() on the parent directory raised
# a genuine AssertionError the moment both a full-load and an incremental
# Bronze object existed under the same table's prefix (see
# docs/analytics-engineering-guide.md, Phase 2, Section 39.6 for the real,
# reproduced error text). These tests write real Parquet files to
# pytest's tmp_path (not this repo's real data/ directory) and prove
# read_bronze_clicks reads both without that error, and returns their
# union.


def _write_single_parquet_file(path, rows: list[dict]) -> None:
    """Write one genuine, single-file Parquet object at `path` -- via
    pyarrow directly, the same way the real production scripts
    (scripts/write_local_bronze_clicks.py,
    scripts/write_local_bronze_clicks_incremental.py) actually write
    Bronze, and deliberately NOT via Spark's own `.write.parquet(...)`,
    which always creates a *directory* of part-files rather than a single
    file at the given path -- using that here would make this test's own
    fixture unrealistic versus what real Bronze objects actually look
    like on disk (this was tried first, and genuinely failed for exactly
    this reason: `list_local_bronze_files`'s glob matched both the
    directory Spark created and the part-file inside it, double-counting
    every row -- a real lesson about matching the test fixture to
    production reality, not a hypothetical concern)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=pa.schema([
        pa.field("id", pa.int64()),
        pa.field("short_code", pa.string()),
        pa.field("occurred_at", pa.timestamp("us")),
        pa.field("device_type", pa.string()),
        pa.field("hashed_ip", pa.string()),
        pa.field("user_id", pa.float64()),
    ]))
    pq.write_table(table, path)


def test_read_bronze_clicks_unions_full_load_and_incremental(spark: SparkSession, tmp_path) -> None:
    full_load_path = tmp_path / "clicks" / "ingestion_date=2026-09-20" / "clicks.parquet"
    incremental_path = (
        tmp_path / "clicks" / "incremental" / "watermark_start=000000000001"
        / "watermark_end=000000000002" / "clicks.parquet"
    )
    _write_single_parquet_file(
        full_load_path,
        [{"id": 1, "short_code": "aaa111", "occurred_at": TS, "device_type": "mobile", "hashed_ip": None, "user_id": None}],
    )
    _write_single_parquet_file(
        incremental_path,
        [{"id": 2, "short_code": "bbb222", "occurred_at": TS, "device_type": "desktop", "hashed_ip": None, "user_id": None}],
    )

    # The real regression: this must NOT raise
    # "AssertionError: Conflicting directory structures detected."
    bronze_df = read_bronze_clicks(spark, tmp_path)

    assert bronze_df.count() == 2
    assert set(bronze_df.columns) == set(BRONZE_CLICKS_SCHEMA.fieldNames())
    assert {row.id for row in bronze_df.collect()} == {1, 2}


def test_read_bronze_clicks_raises_clear_error_when_nothing_exists(spark: SparkSession, tmp_path) -> None:
    with pytest.raises(FileNotFoundError, match="no bronze clicks files found"):
        read_bronze_clicks(spark, tmp_path)
