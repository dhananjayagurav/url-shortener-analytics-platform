"""Shared fixtures for transformations unit tests.

Unlike ingestion's unit tests (SQLite + a mocked S3 client, zero real
infrastructure), a Spark unit test genuinely needs a SparkSession -- there
is no lightweight substitute for "does this DataFrame transform do the
right thing" the way SQLite substitutes for Postgres. `local[1]` mode
starts an in-process, single-threaded Spark instance -- no cluster, no
Docker, nothing external -- so this is still a fast, no-infrastructure
unit test in every sense that matters; only "Spark itself" is real, not
any of Bronze/Silver, real files, or real Postgres.

Session-scoped: starting a SparkSession costs real wall-clock time (JVM
startup); every test in this file's session reuses the same one, the same
way pytest-spark and most real-world Spark test suites do it.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("analytics-transform-unit-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()
