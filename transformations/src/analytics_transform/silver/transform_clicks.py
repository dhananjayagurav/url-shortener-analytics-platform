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

Section 40 added deduplication (`deduplicate_clicks`, below) -- see
docs/analytics-engineering-guide.md, Phase 2, Section 40, for the full
write-up, including why plain key-based dedup (not "pick the freshest
value") is correct for this specific table.

Explicitly NOT here yet: SCD, a full data-quality quarantine framework
(bad rows are coerced/dropped in place, not routed anywhere for review),
and any dimension-key join (`url_key`/`user_key`/`device_key`/`date_key`
-- that's Gold's job, a later Phase 2 milestone).
"""

from __future__ import annotations

import argparse
import functools
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, TimestampType

from analytics_transform.config import get_transform_settings, list_local_bronze_files

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
        # A deliberate, explicit contract for Silver clicks' column set,
        # independent of whatever columns happened to come out of the
        # Bronze read -- kept even though read_bronze_clicks (below) no
        # longer lets a stray partition column reach this function at all
        # (Section 39 fixed that at the read layer, by reading each
        # Bronze object explicitly rather than as one directory -- see
        # that function's docstring for the real, observed reason).
        # Defense-in-depth: if a future Bronze object ever carries an
        # unexpected extra column for some other reason, this still keeps
        # Silver's output schema exactly seven columns, always.
        .select(
            "click_id", "short_code", "occurred_at", "device_type",
            "hashed_ip", "user_id", "silver_loaded_at",
        )
    )


def deduplicate_clicks(silver_df: DataFrame) -> DataFrame:
    """Drop duplicate `click_id`s, keeping exactly one row per id.

    Exists to close a real, reproduced bug (Section 39.7): once more than
    one Bronze full-load snapshot of `clicks` coexists on disk (e.g. two
    separate full-load runs, both still present under
    `bronze/clicks/ingestion_date=.../`), `read_bronze_clicks`'s union
    legitimately contains the *same* click_id more than once -- Spark has
    no way to know that on its own, since nothing before this function
    de-conflicts Bronze objects by content, only by which files exist on
    disk (Section 39's `list_local_bronze_files`).

    Plain `.dropDuplicates(["click_id"])` -- keep any one copy, not
    specifically "the newest one" -- is deliberately sufficient here,
    which is NOT true of every table. It relies on a real, already-stated
    contract fact: `contracts/source/clicks.yaml`'s `quality_rules` say
    "id is monotonically increasing" and "rows are never UPDATEd or
    DELETEd after insert" -- clicks are insert-only/immutable at the
    source. So two Bronze copies of the same click_id are guaranteed
    byte-identical in every business column; there is no "freshest
    version" to pick between, because the row never changes after it's
    first written. (Contrast this with a mutable table like `urls`, where
    two Bronze copies of the same row *can* legitimately differ --
    `title` or `is_active` can change between snapshots -- and a
    freshest-wins strategy, e.g. `row_number() OVER (PARTITION BY id
    ORDER BY silver_loaded_at DESC)`, would be required instead. That's
    future work for whichever Phase 2 section builds Silver `urls`, not
    handled here.)

    Called on the already-cleaned (Silver-shaped) DataFrame, not the raw
    Bronze DataFrame -- see run_silver_clicks_job below for why: keeping
    "rows dropped for being invalid" and "rows dropped for being
    duplicates" as two separate, independently-explainable counts is
    only possible if this runs after clean_clicks, on the `click_id`
    column clean_clicks already produced (Bronze's raw `id` column no
    longer exists by this point).

    This is the pipeline's first WIDE transformation: dropDuplicates
    requires a shuffle to co-locate every row sharing a click_id onto the
    same partition before Spark can tell which copies are duplicates --
    unlike `read_bronze_clicks`'s `unionByName` (narrow, Section 39.7) or
    `clean_clicks`'s per-row `.withColumn`/`.filter` calls (all narrow).
    See docs/analytics-engineering-guide.md, Phase 2, Section 40.7, for a
    real, captured `EXPLAIN (formatted)` showing the `Exchange` node this
    introduces -- and the honest correction to Section 37.7, which
    predicted the first shuffle wouldn't happen until the Gold-layer
    dimension join (Section 43).
    """
    return silver_df.dropDuplicates(["click_id"])


def read_bronze_clicks(spark: SparkSession, bronze_root: Path) -> DataFrame:
    """Read every real Bronze `clicks` object -- full-load and
    incremental batches together -- as one unioned DataFrame.

    Reads each file returned by `list_local_bronze_files` *individually*
    (`spark.read.parquet(str(f))` per file) and unions the results, rather
    than calling `spark.read.parquet(bronze_root / "clicks")` once on the
    parent directory. This is not a style preference -- pointing Spark at
    the directory genuinely fails, the moment both a full-load object
    (`ingestion_date=.../clicks.parquet`) and an incremental object
    (`incremental/watermark_start=.../watermark_end=.../clicks.parquet`)
    exist under it at once:

        AssertionError: Conflicting directory structures detected.
        ... If provided paths are partition directories, please set
        "basePath" in the options ... If there are multiple root
        directories, please load them separately and then union them.

    -- a real, genuinely-reproduced error (see
    docs/analytics-engineering-guide.md, Phase 2, Section 39.6),
    triggered by Spark's own Hive-style partition-column discovery
    trying, and failing, to reconcile Bronze's two different partitioning
    schemes (Section 14's `ingestion_date=`, Section 15's
    `watermark_start=`/`watermark_end=`) as one table. Reading each file
    by its own explicit path sidesteps partition discovery entirely --
    verified directly (Section 39.6 again): neither file gains a stray
    `ingestion_date` or `watermark_start`/`watermark_end` column when read
    this way, so the two DataFrames' schemas already match and
    `unionByName` needs no further reconciliation.
    """
    files = list_local_bronze_files(bronze_root, "clicks")
    if not files:
        raise FileNotFoundError(f"no bronze clicks files found under {bronze_root / 'clicks'}")

    dataframes = [spark.read.parquet(str(f)) for f in files]
    return functools.reduce(lambda left, right: left.unionByName(right), dataframes)


def run_silver_clicks_job(spark: SparkSession, bronze_root: Path, silver_path: str) -> dict[str, int]:
    """Read every real Bronze clicks object, clean it, deduplicate it,
    write Silver clicks Parquet. Returns real counts -- this is the dict
    the CLI entrypoint prints, and the numbers the guide's "How to
    Verify" section shows are the actual output of running this
    function, not typed-up estimates.

    Three counts, not one conflated `dropped_rows` (that field existed
    through Section 39; Section 40 splits it, because "invalid" and
    "duplicate" are different failure modes with different causes and
    different fixes, and collapsing them into one number hides which one
    actually happened on a given run):

    - `invalid_rows_dropped`: rows clean_clicks's filters removed --
      missing click_id, missing occurred_at, empty short_code. A data
      QUALITY problem, at the source or in Bronze.
    - `duplicate_rows_removed`: rows deduplicate_clicks's dropDuplicates
      removed -- the same click_id appearing in more than one Bronze
      object. A pipeline/orchestration problem (Section 39.7: multiple
      overlapping Bronze snapshots coexisting), not a data quality one.
    """
    bronze_df = read_bronze_clicks(spark, bronze_root)
    bronze_count = bronze_df.count()

    cleaned_df = clean_clicks(bronze_df)
    cleaned_count = cleaned_df.count()

    silver_df = deduplicate_clicks(cleaned_df)
    silver_count = silver_df.count()

    silver_df.write.mode("overwrite").parquet(silver_path)

    stats = {
        "bronze_rows": bronze_count,
        "invalid_rows_dropped": bronze_count - cleaned_count,
        "duplicate_rows_removed": cleaned_count - silver_count,
        "silver_rows": silver_count,
    }
    logger.info("silver clicks job complete", extra=stats)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()

    settings = get_transform_settings()
    silver_path = str(settings.silver_root / "clicks")

    spark = SparkSession.builder.appName("silver-clicks").master("local[*]").getOrCreate()
    try:
        stats = run_silver_clicks_job(spark, settings.bronze_root, silver_path)
        print(stats)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
