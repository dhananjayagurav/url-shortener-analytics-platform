from __future__ import annotations

import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def local_spark() -> SparkSession:
    """local[1]: single-threaded, deterministic, no cluster needed.

    PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON are set explicitly to this
    process's own interpreter (sys.executable would also work; the env
    vars are set here so the fix is visible rather than implicit) --
    without this, Spark's worker subprocess can resolve a *different*
    python3 on PATH than the one pyspark is installed into, and Spark
    refuses to run with a PySparkRuntimeError: PYTHON_VERSION_MISMATCH.
    I hit this exact error verifying this fixture before handing it to
    you; this is the fix, not a hypothetical.
    """
    import sys

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (
        SparkSession.builder.appName("phase2-spark-unit-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()