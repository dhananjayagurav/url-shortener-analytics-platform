"""Outcome 1: read one Bronze full-load batch of `clicks`, apply narrow
transformations, and trigger two actions -- enough to observe the DAG,
stages/tasks, and the transformation-vs-action boundary directly.

Deliberately reads ONLY the ingestion_date=* (full-load) partitions, not
the incremental/watermark_start=.../watermark_end=... partitions that also
exist under bronze/clicks/ -- reading both together trips a real Spark
partition-discovery conflict (two different partition-column schemes under
one prefix). Reconciling full-load and incremental batches into one
consistent read is Outcome 4/5 material, not this one's.
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, functions as F

from phase2_spark.config import get_phase2_settings 
from phase2_spark.spark_session import build_spark_session

def build_valid_clicks(clicks_df: DataFrame) -> DataFrame:
    """Two narrow transformations: filter (drop rows with no id) and
    select+withColumn (project columns, normalize device_type casing).

    Narrow = each output partition depends on exactly one input partition.
    No data crosses the network between executors to compute this -- that
    property is exactly what "narrow" means, and it's why this function
    alone can't demonstrate a shuffle (Outcome 2's job will).
    """
    return(
        clicks_df.filter(F.col("id").isNotNull())
        .select("id", "short_code", "occurred_at", "device_type")
        .withColumn("device_type", F.lower(F.col("device_type")))
    )

def main() -> None:
    settings = get_phase2_settings()
    spark = build_spark_session(settings)

    bronze_path = f"s3a://{settings.minio_bucket}/bronze/clicks/ingestion_date=*/clicks.parquet"

    # Nothing has executed yet. This line only registers a plan
    click_df = spark.read.parquet(bronze_path)
    valid_clicks = build_valid_clicks(click_df)

    # ACTION 1: forces a real job. Spark must actually read the Parquet
    # file and run the filter/select/withColumn to produce a count.
    row_count = valid_clicks.count()
    print(f"Valid clicks: ", {row_count})

    # ACTION 2: a second, separate job. Unless you .cache() valid_clicks
    # (Outcome 3), Spark reads the Parquet file and reruns the
    # transformations again here -- it does not remember action 1's result.
    valid_clicks.write.mode("overwrite").parquet(
        f"s3a://{settings.minio_bucket}/phase2/explore_ckicks_output/"
    )
    print("wrote output to phase2/explore_clicks_output/")

    spark.stop()

if __name__ == "__main__":
    sys.exit(main() or 0)


