"""Bronze `clicks` -> Silver `clicks`: Phase 2's first transformation.

See docs/analytics-engineering-guide.md, Phase 2, Section 38, for the full
concept/architecture/design-decision write-up. This module deliberately
does four things and no more (see that section's "Design Decision" for
why each of these, and only these, belong here):

1. Schema normalization -- rename `id` to `click_id` (matching the column
   name `sql/analytics/005_fact_clicks.sql` already commits to for Gold),
   and cast every column to its intended Silver type.
2. Timestamp normalization -- `occurred_at` becomes a proper Spark
   TimestampType column, not whatever the source driver happened to hand
   back.
3. String trimming -- `short_code`, `device_type`, `hashed_ip`.
4. Basic validity checks -- an unrecognized `device_type` is coerced to
   the contract's own `'unknown'` member rather than dropped (matching
   `dim_device`'s existing Unknown-member convention, Section 10.3); a
   malformed `hashed_ip` is nulled out, not the whole row; a row missing
   its primary key, its business timestamp, or its `short_code` is
   dropped (those three are not recoverable -- there's no default that
   makes a click event meaningful without them).

Explicitly NOT here yet: deduplication, SCD, a full data-quality
quarantine framework (bad rows are coerced/dropped in place, not routed
anywhere for review), and any dimension-key join (`url_key`/`user_key`/
`device_key`/`date_key` -- that's Gold's job, a later Phase 2 milestone).
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, TimestampType

from analytics_transform.config import get_transform_settings

logger = logging.getLogger(__name__)

# Matches contracts/source/clicks.yaml's quality_rules and
# sql/analytics/004_dim_device.sql's seeded members exactly -- see
# schemas/source/clicks.md for why 'unknown' exists even though the
# seeded source data never produces it today.
VALID_DEVICE_TYPES = ("mobile", "desktop", "tablet", "unknown")

# contracts/source/clicks.yaml: "hashed_ip, if present, is a 64-character
# hex string (SHA-256)". Same rule, enforced here instead of only being
# documented.
HASHED_IP_PATTERN = r"^[0-9a-f]{64}$"


def clean_clicks(bronze_df: DataFrame) -> DataFrame:
    """Pure transform: Bronze clicks DataFrame in, Silver clicks DataFrame
    out. No I/O -- see run_silver_clicks_job for the read/write wrapper.
    Kept pure specifically so unit tests can build a small in-memory
    DataFrame with deliberately dirty rows and assert on the result
    directly, the same separation ingestion/extract_full.py's own
    `extract_full` (pure) vs `run_full_load` (I/O) draws.
    """
    return (
        bronze_df
        # --- schema normalization ---
        .withColumnRenamed("id", "click_id")
        .withColumn("click_id", F.col("click_id").cast(LongType()))
        .withColumn("user_id", F.col("user_id").cast(LongType()))
        # --- timestamp normalization ---
        .withColumn("occurred_at", F.col("occurred_at").cast(TimestampType()))
        # --- string trimming ---
        .withColumn("short_code", F.trim(F.col("short_code")))
        .withColumn("device_type", F.trim(F.lower(F.col("device_type"))))
        .withColumn("hashed_ip", F.trim(F.col("hashed_ip")))
        # --- basic validity checks ---
        .withColumn(
            "device_type",
            F.when(F.col("device_type").isin(list(VALID_DEVICE_TYPES)), F.col("device_type"))
            .otherwise(F.lit("unknown")),
        )
        .withColumn(
            "hashed_ip",
            F.when(F.col("hashed_ip").rlike(HASHED_IP_PATTERN), F.col("hashed_ip"))
            .otherwise(F.lit(None).cast(StringType())),
        )
        .filter(F.col("click_id").isNotNull())
        .filter(F.col("occurred_at").isNotNull())
        .filter((F.col("short_code").isNotNull()) & (F.length(F.col("short_code")) > 0))
        .withColumn("silver_loaded_at", F.current_timestamp())
        # --- explicit output schema ---
        # Spark's parquet reader auto-discovers Hive-style partition
        # columns from the *directory* names it reads (see
        # run_silver_clicks_job's docstring): reading
        # bronze/clicks/ingestion_date=2026-09-20/clicks.parquet adds an
        # `ingestion_date` column that isn't in the Parquet file itself,
        # and doesn't exist on an incremental Bronze read at all (those
        # objects partition by `watermark_start=`/`watermark_end=`
        # instead -- object_store.py's build_bronze_incremental_key). This
        # final .select() is deliberate, not incidental: it fixes Silver's
        # column set regardless of which partitioning scheme the Bronze
        # read picked up, so Silver clicks has one stable schema no matter
        # which Bronze objects fed it. See Section 38's Failure Scenario
        # for what breaks if this line is removed.
        .select(
            "click_id", "short_code", "occurred_at", "device_type",
            "hashed_ip", "user_id", "silver_loaded_at",
        )
    )


def run_silver_clicks_job(spark: SparkSession, bronze_path: str, silver_path: str) -> dict[str, int]:
    """Read Bronze clicks Parquet, clean it, write Silver clicks Parquet.
    Returns real counts (bronze rows read, silver rows written, rows
    dropped) -- this is the dict the CLI entrypoint prints, and the
    numbers the guide's "How to Verify" section shows are the actual
    output of running this function, not typed-up estimates.
    """
    bronze_df = spark.read.parquet(bronze_path)
    bronze_count = bronze_df.count()

    silver_df = clean_clicks(bronze_df)
    silver_count = silver_df.count()

    silver_df.write.mode("overwrite").parquet(silver_path)

    stats = {
        "bronze_rows": bronze_count,
        "silver_rows": silver_count,
        "dropped_rows": bronze_count - silver_count,
    }
    logger.info("silver clicks job complete", extra=stats)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()

    settings = get_transform_settings()
    bronze_path = str(settings.bronze_root / "clicks")
    silver_path = str(settings.silver_root / "clicks")

    spark = SparkSession.builder.appName("silver-clicks").master("local[*]").getOrCreate()
    try:
        stats = run_silver_clicks_job(spark, bronze_path, silver_path)
        print(stats)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
