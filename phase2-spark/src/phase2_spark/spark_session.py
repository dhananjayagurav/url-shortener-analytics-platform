"""The one place Spark + S3A configuration is assembled.

Every Phase 2 job calls build_spark_session() rather than constructing a
SparkSession itself -- see docs Outcome 1, Design Decision.
"""

from __future__ import annotations

from pyspark.sql import SparkSession 

from phase2_spark.config import Phase2Settings 

def build_spark_session(settings: Phase2Settings, *, master: str | None = None) -> SparkSession:
    """Build a SparkSession wired to talk to MinIO over S3A.

    IMPORTANT: spark.jars.packages is deliberately NOT set here. When this
    function runs inside a process launched by `spark-submit` (the real
    path -- see the Makefile's explore-clicks target), the driver JVM has
    ALREADY started, with its classpath already fixed, by the time this
    Python code executes. spark.jars.packages controls what gets added to
    that classpath -- but by the time .config() runs here, it's too late;
    a running JVM's classpath can't be changed after the fact. This has to
    be passed to `spark-submit --packages ...` on the command line
    instead, where Spark's own submission bootstrap resolves it via Ivy
    BEFORE the JVM starts. The S3A settings below are different: Hadoop
    reads fs.s3a.* properties lazily, the first time an s3a:// path is
    actually accessed -- well after the JVM is already running -- so
    setting those here, programmatically, is fine.

    hadoop-aws:3.3.4 + aws-java-sdk-bundle:1.12.262 are not arbitrary
    versions: PySpark 3.5.9 bundles Hadoop client 3.3.4, and Hadoop 3.3.4's
    own pom.xml pins aws-java-sdk-bundle to exactly 1.12.262.
    """
    builder = (
        SparkSession.builder.appName(settings.spark_app_name)
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




