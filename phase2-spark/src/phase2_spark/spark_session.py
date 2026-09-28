"""The one place Spark + S3A configuration is assembled.

Every Phase 2 job calls build_spark_session() rather than constructing a
SparkSession itself -- see docs Outcome 1, Design Decision.
"""

from __future__ import annotations

from pyspark.sql import SparkSession 

from phase2_spark.config import Phase2Settings 

def build_spark_session(settings: Phase2Settings, *, master: str | None = None) -> SparkSession:
    """Build a SparkSession wired to talk to MinIO over S3A.

    `master` defaults to settings.spark_master_url (the real cluster, via
    docker-compose's spark-master service). Tests override it to
    "local[1]"/"local[*]" -- see tests/unit/conftest.py -- which is why
    `master` is a parameter here rather than hardcoded.

    hadoop-aws:3.3.4 + aws-java-sdk-bundle:1.12.262 are not arbitrary
    versions: PySpark 3.5.9 bundles Hadoop client 3.3.4 (confirmed by
    inspecting the installed jars in this sandbox), and Hadoop 3.3.4's own
    pom.xml pins aws-java-sdk-bundle to exactly 1.12.262. A mismatched
    aws-java-sdk-bundle version here is a common, confusing source of
    NoSuchMethodError at runtime -- pinning both together avoids it.
    """
    builder = (
        SparkSession.builder.appName(settings.spark_app_name)
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )
    if master is not None:
        builder = builder.master(master)
    else:
        builder = builder.master(settings.spark_master_url)
    return builder.getOrCreate()




