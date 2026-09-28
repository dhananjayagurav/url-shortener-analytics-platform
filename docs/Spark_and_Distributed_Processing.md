Phase 2 — Spark & Distributed Data Processing
Purpose of this document

This is the Phase 2 companion to docs/analytics-engineering-guide.md. Phase 1's guide covers the OLTP-mirrored source schema, ingestion, contracts, checkpointing, and the Bronze object-storage layer — all built on pandas + boto3, running in one process.

Phase 2 is about distributed processing with Apache Spark, built as a fresh, separately-named module (phase2-spark/, package phase2_spark) rather than an extension of Phase 1's ingestion/ package or the earlier transformations/analytics_transform exploration. Nothing in phase2-spark/ imports from either of those. Shared infrastructure — docker-compose.yml, .env, pyproject.toml's dependency declarations — is extended, not duplicated, where it's genuinely common (e.g. the same MinIO instance Phase 1 writes Bronze to).

Each Learning Outcome below follows the same shape: CONCEPT, PROJECT EXAMPLE, ENVIRONMENT, DESIGN, IMPLEMENTATION, CODE WALKTHROUGH, RUN & OBSERVE, EXPERIMENT, PRODUCTION VIEW, PRINCIPAL ENGINEER VIEW, REMEMBER. Follow-up deep dives and troubleshooting notes from actually running the material are appended under each Outcome, in the order they came up.

Status
CURRENT PHASE:   Phase 2 — Spark & Distributed Data Processing
CURRENT OUTCOME: 1 of 10 — SPARK EXECUTION FUNDAMENTALS (delivered, with follow-up deep dives)
COMPLETED:       Outcome 1
NEXT:            Outcome 2 — Shuffle & Distributed Processing

Outcomes 2–10 (Shuffle & Distributed Processing; Spark Performance & Diagnosis; Data Lake Transformation Architecture; Data Correctness & Transformation; Data Quality Framework; Analytical Serving; Query & Data Performance; Phase 2 Production Engineering; Phase 2 Integration & System View) will be appended to this document as sections, in the same shape, as they're taught.

Outcome 1 — Spark Execution Fundamentals
1. Concept

Everything else in Phase 2 — shuffles, joins, caching, quarantine logic, data quality checks — is built on top of four ideas, and if any one of them is fuzzy, every later debugging session gets harder.

Driver vs Executors. When you run a Spark job, one JVM process (the driver) runs your main() code, builds up a plan, and asks a cluster manager for workers. Separate JVM processes (executors) actually hold data partitions in memory and run the compute. The driver never processes rows itself — it only ever schedules work. This matters immediately for your pipeline: pd.read_sql_table in extract_full.py (Phase 1) pulls the entire result into the driver's own memory as a single pandas DataFrame — there's no "driver vs executor" split at all. Phase 2 is the first place in this project where that split exists, and it's the reason a 50M-row table that would OOM a Phase-1-style pandas extraction can be processed by Spark without OOMing the driver: the rows never all live in one process at once.

DAG, stages, tasks, partitions. A Spark job is a directed acyclic graph (DAG) of operations. Spark doesn't run each line of your code as you write it — it builds the whole DAG first, then compiles it into stages (groups of operations that can run without moving data between machines), and each stage is split into tasks — one task per partition of the data. A partition is simply a chunk of the dataset that fits in one executor core's memory and is processed independently. If your Bronze Parquet file is read as 4 partitions and you have 4 executor cores, you get 4 tasks running in parallel, one per partition.

Transformations vs actions. Spark's DataFrame API has two kinds of operations. Transformations (.filter(), .select(), .withColumn(), .join()) describe a new DataFrame in terms of an existing one — Spark records this as a step in the DAG and does no actual computation. Actions (.count(), .collect(), .write.parquet(), .show()) are the only operations that trigger real execution — they force Spark to actually run the DAG built up to that point, producing a concrete result.

Lazy evaluation is the reason that split exists: nothing runs until an action is called. This isn't an implementation detail you can ignore — it's the reason a stack trace from a .write.parquet() call can point at a .filter() you wrote 10 lines earlier: the filter didn't fail when you wrote it, because it didn't run when you wrote it.

2. Project Example

Phase 1's pipeline already produces real Bronze data that's perfect for this: ingestion/src/url_shortener_analytics/object_store.py's build_bronze_key() wrote a full-load snapshot of the clicks table to a deterministic key, confirmed on disk at:

data/bronze/clicks/ingestion_date=2026-09-20/clicks.parquet

(In the real MinIO bucket this is the object key bronze/clicks/ingestion_date=2026-09-20/clicks.parquet in the analytics-lake bucket — data/bronze/ on disk is this local sandbox's mirror of what MinIO actually holds.)

Outcome 1's job reads exactly this file with Spark, applies two narrow transformations, and calls two actions — enough to see a DAG, stages, tasks, and the lazy-vs-eager boundary directly, without yet touching shuffle (that's Outcome 2).

The job deliberately reads only the ingestion_date=* (full-load) partitions, not the incremental/watermark_start=.../watermark_end=... partitions that also exist under bronze/clicks/. Reading both at once with a single spark.read.parquet("bronze/clicks/") call hits a real bug: Spark's Hive-style partition discovery tries to infer one consistent partition-column schema for everything under a directory, and ingestion_date= vs watermark_start=/watermark_end= are two different schemes, which throws AssertionError: Conflicting directory structures detected. That's a real, already-discovered issue in this Bronze layout — it's Outcome 4/5 material (reconciling full-load and incremental batches into one Silver table), not Outcome 1's. Here it's sidestepped honestly with a glob scoped to only the full-load partition:

bronze/clicks/ingestion_date=*/clicks.parquet
3. Environment

Verified directly, in a sandbox, before writing anything below:

PySpark is 3.5.9, installed via Phase 1's transform optional-dependency group (pyproject.toml).
The official Docker Hub spark image has a tag 3.5.9-python3 matching that installed version exactly.
The actual Apache spark-docker repo source for that image: SPARK_HOME=/opt/spark, WORKDIR /opt/spark/work-dir, ENTRYPOINT ["/opt/entrypoint.sh"]. The entrypoint script's routing logic is a case statement — driver and executor are handled specially (Kubernetes-oriented), and anything else falls through to exec "$@". That's why a docker-compose command: override like ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"] runs exactly as given.
A genuine environment bug hit while self-testing the transformation logic below, fixed before showing it: PySpark's worker subprocess was resolving a bare python3 on $PATH to system Python 3.11, while the driver (a .venv) was Python 3.12 — a real PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON mismatch (PySparkRuntimeError: [PYTHON_VERSION_MISMATCH]). Fix: explicitly export PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to the exact same interpreter path.

Not run in that sandbox: the actual spark-master/spark-worker containers, a real spark-submit against them, or a real S3A read from MinIO (no Docker daemon there). Those commands were correct against verified facts, but running them for real and observing the Spark UI happens on your own machine — see the Troubleshooting Log at the end of this Outcome for what actually happened when you did.

4. Design
phase2-spark/
├── src/
│   └── phase2_spark/
│       ├── __init__.py
│       ├── config.py            # Phase2Settings: cluster + S3A config, separate from Phase 1's Settings
│       ├── spark_session.py     # build_spark_session(): the one place S3A/Spark config is assembled
│       └── jobs/
│           ├── __init__.py
│           └── explore_clicks.py  # this outcome's job: read Bronze, transform, act
└── tests/
    └── unit/
        ├── conftest.py           # local[*] SparkSession fixture, scoped to phase2-spark only
        └── test_explore_clicks.py

Design decisions worth calling out explicitly:

A separate Phase2Settings, not Phase 1's Settings. Phase 1's config.py has fields like database_url that Spark jobs have no business depending on. Phase2Settings reads the same .env file (so MINIO_ENDPOINT, MINIO_ACCESS_KEY, etc. aren't duplicated as literals) but is its own BaseSettings subclass. Common infrastructure (the .env file, the pydantic-settings pattern) is reused without reusing Phase 1 code (the Settings class itself).
spark_session.py is a single function, build_spark_session(). Every Phase 2 job needs identical S3A wiring; centralizing it means later outcomes' jobs don't re-derive it.
explore_clicks.py has a pure function (build_valid_clicks) separated from I/O (main) — the same shape Phase 1 already uses. It's what makes the unit test possible without a cluster or MinIO: build_valid_clicks takes a DataFrame and returns a DataFrame, so a test can hand it a tiny in-memory DataFrame under local[1] and assert on the result.
Docker Compose gets two new services (spark-master, spark-worker), added to the existing docker-compose.yml, not a second compose file — one Compose project, one make up. This is genuinely shared infrastructure, fitting the phase-isolation rule's own carve-out for common infra. The services join the same default network Postgres and MinIO already use, so spark://spark-master:7077 and http://minio:9000 both resolve via Docker's service-name DNS.
pyproject.toml's existing transform extra (pyspark==3.5.9) is reused as-is, with only the packaging metadata extended to also discover phase2_spark.
5. Implementation
NEW — phase2-spark/src/phase2_spark/__init__.py
python
"""Phase 2: Spark-based distributed processing for the analytics platform.

Deliberately separate from `url_shortener_analytics` (Phase 1, ingestion)
and from `analytics_transform` (an earlier, local-filesystem-only Phase 2
exploration under transformations/). This package assumes a real Spark
cluster (docker-compose's spark-master/spark-worker) and real S3A access
to MinIO -- see spark_session.py.
"""
NEW — phase2-spark/src/phase2_spark/config.py
python
"""Phase 2 settings: cluster + object-store config for Spark jobs.

Deliberately a *separate* pydantic-settings class from Phase 1's
url_shortener_analytics.config.Settings, even though both read the same
.env file. Phase 1's Settings carries OLTP fields (database_url) that no
Spark job needs, and importing it here would make Phase 2 depend on
Phase 1's class staying stable -- see Design Decision above.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Phase2Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Object store (same MinIO instance Phase 1 writes Bronze to)
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "labadmin"
    minio_secret_key: str = "labpassword"
    minio_bucket: str = "analytics-lake"

    # Spark cluster
    spark_master_url: str = "spark://localhost:7077"
    spark_app_name: str = "phase2-spark"


@lru_cache
def get_phase2_settings() -> Phase2Settings:
    """Cached the same way Phase 1's get_settings() is. Same staleness
    caveat applies: see Phase 1's lru_cache discussion if you need to
    reload settings mid-process (e.g. in a long-lived notebook)."""
    return Phase2Settings()
NEW — phase2-spark/src/phase2_spark/spark_session.py
python
"""The one place Spark + S3A configuration is assembled.

Every Phase 2 job calls build_spark_session() rather than constructing a
SparkSession itself -- see Design Decision above.
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
NEW — phase2-spark/src/phase2_spark/jobs/explore_clicks.py
python
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
    return (
        clicks_df.filter(F.col("id").isNotNull())
        .select("id", "short_code", "occurred_at", "device_type")
        .withColumn("device_type", F.lower(F.col("device_type")))
    )


def main() -> None:
    settings = get_phase2_settings()
    spark = build_spark_session(settings)

    bronze_path = f"s3a://{settings.minio_bucket}/bronze/clicks/ingestion_date=*/clicks.parquet"

    # Nothing has executed yet -- this line only registers a plan.
    clicks_df = spark.read.parquet(bronze_path)
    valid_clicks = build_valid_clicks(clicks_df)

    # ACTION 1: forces a real job. Spark must actually read the Parquet
    # file and run the filter/select/withColumn to produce a count.
    row_count = valid_clicks.count()
    print(f"valid clicks: {row_count}")

    # ACTION 2: a second, separate job. Unless you .cache() valid_clicks
    # (Outcome 3), Spark reads the Parquet file and reruns the
    # transformations again here -- it does not remember action 1's result.
    valid_clicks.write.mode("overwrite").parquet(
        f"s3a://{settings.minio_bucket}/phase2/explore_clicks_output/"
    )
    print("wrote output to phase2/explore_clicks_output/")

    spark.stop()


if __name__ == "__main__":
    sys.exit(main() or 0)
NEW — phase2-spark/tests/unit/conftest.py
python
from __future__ import annotations

import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def local_spark() -> SparkSession:
    """local[1]: single-threaded, deterministic, no cluster needed.

    PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON are set explicitly to this
    process's own interpreter -- without this, Spark's worker subprocess
    can resolve a *different* python3 on PATH than the one pyspark is
    installed into, and Spark refuses to run with a PySparkRuntimeError:
    PYTHON_VERSION_MISMATCH. This exact error was hit verifying this
    fixture; this is the fix, not a hypothetical.
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
NEW — phase2-spark/tests/unit/test_explore_clicks.py
python
from __future__ import annotations

from phase2_spark.jobs.explore_clicks import build_valid_clicks


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

This exact test was run in a sandbox (local[1], with the PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON fix applied) and genuinely passed:

test_explore_clicks.py::test_build_valid_clicks_drops_null_ids_and_lowercases_device_type PASSED
1 passed in 8.42s
MODIFIED — docker-compose.yml (add two services; nothing existing changes)

See the Troubleshooting Log at the end of this Outcome — the version below was superseded by a corrected one once a real ModuleNotFoundError showed up running this for real. Use the corrected version from that section, not this one, when setting up your cluster.

yaml
  spark-master:
    image: spark:3.5.9-python3
    container_name: analytics-platform-spark-master
    restart: unless-stopped
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
    ports:
      - "7077:7077"   # Spark master RPC -- workers and spark-submit connect here
      - "8090:8080"   # Spark master web UI (mapped to 8090 to avoid clashing with other local services on 8080)

  spark-worker:
    image: spark:3.5.9-python3
    container_name: analytics-platform-spark-worker
    restart: unless-stopped
    depends_on:
      - spark-master
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]
    ports:
      - "8091:8081"   # Spark worker web UI
    environment:
      SPARK_WORKER_CORES: "2"
      SPARK_WORKER_MEMORY: "2g"
MODIFIED — .env.example (append)
bash
# --- Phase 2: Spark ---
# spark://spark-master:7077 when run from inside the Docker network
# (docker compose exec spark-master spark-submit ...); spark://localhost:7077
# does not work for this because executors also need to resolve this same
# address, and "localhost" from inside a spark-worker container means that
# container, not your host machine.
SPARK_MASTER_URL=spark://spark-master:7077
MODIFIED — pyproject.toml (two small edits)
diff
 [tool.setuptools.packages.find]
-where = ["ingestion/src", "transformations/src"]
-include = ["url_shortener_analytics*", "analytics_transform*"]
+where = ["ingestion/src", "transformations/src", "phase2-spark/src"]
+include = ["url_shortener_analytics*", "analytics_transform*", "phase2_spark*"]

The transform extra (pyspark==3.5.9) is untouched — it already installs the one dependency phase2_spark needs too.

MODIFIED — Makefile (two new targets; nothing existing changes)

See the Troubleshooting Log — the explore-clicks target below was also superseded; use the corrected version.

makefile
# Phase 2 (fresh module) -- separate from test-transform, which runs the
# earlier transformations/analytics_transform package's own tests.
test-phase2-spark:
	PYTHONPATH=phase2-spark/src python3 -m pytest phase2-spark/tests/unit -v

# Must run *inside* the Docker network so the driver and executors resolve
# spark-master/minio identically -- see Design.
explore-clicks:
	docker compose exec spark-master \
		/opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.pyspark.python=python3 \
		phase2-spark/src/phase2_spark/jobs/explore_clicks.py
6. Code Walkthrough

The one thing worth pausing on that isn't obvious syntax: why does main() build clicks_df and valid_clicks before calling any action, and why does calling .count() and then .write.parquet() mean the Parquet file gets read twice?

spark.read.parquet(bronze_path) doesn't read anything. It returns a DataFrame object that's really just a pointer to a not-yet-executed plan: "read Parquet from this path." build_valid_clicks() extends that plan with a filter, a select, and a withColumn — still no execution, still just plan-building. By the time main() reaches row_count = valid_clicks.count(), the full plan is: read → filter → select → withColumn → count. Calling .count() is what turns that plan into an actual Spark job: the driver submits it, gets it split into stages and tasks, executors run it, and a single integer comes back.

The next line, .write.parquet(...), is a second, independent call to the same valid_clicks object. Spark does not remember that it already computed this DataFrame's rows during the .count() call — DataFrames aren't materialized results, they're re-runnable plans. So .write.parquet() triggers a second job that reads the Parquet file from MinIO again and reruns the filter/select/withColumn again. This is not a bug — it's the direct, sometimes-expensive consequence of laziness, and it's exactly what Outcome 3's .cache()/.persist() material exists to fix.

7. Run & Observe
bash
# 1. Bring up Postgres, MinIO, and now also spark-master/spark-worker
docker compose up -d

# 2. Confirm the worker actually registered with the master
#    Open http://localhost:8090 -- you should see 1 worker listed under
#    "Workers", with 2 cores / 2g memory (from SPARK_WORKER_CORES/
#    SPARK_WORKER_MEMORY above).

# 3. Install the phase2_spark package (same pyspark dependency Phase 1's
#    transform extra already declares)
make install-transform

# 4. Run the unit test (no cluster needed -- local[1])
make test-phase2-spark

# 5. Run the real job against the real cluster
make explore-clicks

What to actually look at, not just run:

Spark UI at http://localhost:8090 while make explore-clicks is running. Click into the running/completed application. You should see two separate jobs — one triggered by .count(), one by .write.parquet(). Click into either job and you'll see its stage(s) and, inside a stage, one task per partition of the Bronze file that was read.
Terminal output from make explore-clicks should print valid clicks: <N> followed by wrote output to phase2/explore_clicks_output/.
MinIO console at http://localhost:9001 — confirm analytics-lake/phase2/explore_clicks_output/ now has Parquet part-files in it.
8. Experiment

Comment out the .write.parquet(...) call (job 2), leaving only the .count() action, and rerun make explore-clicks. Expected observation: the Spark UI now shows exactly one job instead of two, and MinIO's phase2/explore_clicks_output/ prefix does not get created/updated. This directly demonstrates that each action is an independent trigger of the DAG.

9. Production View
Correctness: narrow transformations over one immutable Bronze file are deterministic and side-effect-free per run.
Performance: the double-read/double-recompute exposed above is a real performance issue, not just a teaching device — in a large pipeline with several actions over the same DataFrame, this pattern multiplies compute cost by the number of actions. Outcome 3 (caching/persistence) is the direct fix.
Reliability: this job has zero error handling — appropriate for this outcome's focus on DAG mechanics, but not acceptable as real production code; Outcome 9 addresses this properly.
Scalability: parallelism today is bounded by how many partitions spark.read.parquet creates when reading one small Parquet file — likely 1, given the file's small size, meaning most of the worker's 2 cores sit idle.
Cost: running a full Spark cluster for a job this small is genuine overkill in isolation — justified here purely because the cluster and its execution model are the thing being taught.
10. Principal Engineer View
"Why does my Spark job seem to hang and then everything happens at once?" is a direct, predictable consequence of laziness — Catalyst needs to see the whole plan before running anything, enabling optimizations like predicate pushdown that eager execution would rule out.
The driver/executor split is what makes Spark scale past the memory of one machine — and what makes it a distributed system, with distributed-systems failure modes: partial failures, network calls between driver and executors, non-determinism in task scheduling order.
"How many partitions does this DataFrame have, and why?" is a question every senior Spark engineer should be able to answer for any DataFrame they're looking at (df.rdd.getNumPartitions()), because partition count directly determines task count, which directly determines parallelism.
A DAG is a contract, not a suggestion. A .filter() referencing a nonexistent column won't raise AnalysisException until an action runs it — the stack trace's line number can be far from the actual mistake.
11. Remember
Transformations build a plan; actions run it. Nothing executes until an action is called.
The driver schedules; executors compute. The driver never processes your actual data rows.
A DAG compiles into stages (data-movement boundaries) and stages compile into tasks (one per partition).
Calling two actions on the same un-cached DataFrame reruns the whole upstream plan twice — expected behavior, and the reason caching exists.
"It ran" and "it ran once" are different claims — always know how many actions you called before asking why something took as long as it did.
Deep Dive — Driver/Executor, DAG/Stages/Tasks/Partitions, Transformations vs Actions, Lazy Evaluation

(Requested as a follow-up: the concepts above needed more depth for interview prep, junior through principal level.)

1. Why Spark exists at all

The problem. One machine has a fixed amount of RAM and a fixed number of CPU cores. If your data is bigger than one machine's RAM, you cannot load it all into one Python process. Pandas assumes the whole dataset fits in the memory of the machine running it.

Analogy. Counting every book in a giant library, alone, takes all day. Bring 20 friends, split the library into 20 sections, everyone counts their section at once, then add up 20 numbers at the end. The job finishes far faster, and no single person had to hold the whole library's inventory in their head.

What Spark actually is. Spark splits a large dataset into chunks, sends each chunk to a different machine, and each machine works on its own chunk at the same time. Spark also tracks exactly what happened, so if one machine crashes partway through, Spark redoes just that machine's piece of work instead of restarting everything.

Project tie-in. extract_full() (Phase 1) calls pd.read_sql_table(...), pulling the entire clicks table into one pandas DataFrame, in one process, on one machine. Fine for a few thousand rows. It would fail outright for 500 million rows, because no single machine's RAM could hold it. Phase 2 exists because the pipeline needs to survive that scale.

2. Driver and Executors

Analogy: a construction site. A foreman doesn't lay bricks. The foreman reads the blueprint, decides which worker does which part, and checks on progress. Workers lay the bricks. Blueprint = your Spark code. Foreman = the driver. Workers = executors. The staffing agency that finds available workers = the cluster manager.

What the driver does, precisely:

Builds the plan (the DAG) from your DataFrame code.
Asks the cluster manager for executors (count, cores, memory each).
Schedules work: breaks the plan into tasks, sends each to an executor.
Collects results: a count, a .collect() list, and so on, travel back to the driver.

The driver does not hold your actual dataset — only the plan and small results. If the driver dies, the entire application dies immediately — there is exactly one driver, and it's a single point of failure.

What an executor does: holds actual partitions in memory (or spills to disk), runs the actual computation, reports progress/results to the driver, can cache data across actions (Outcome 3). If an executor dies mid-job, the driver notices the missing heartbeat and reschedules that executor's unfinished tasks elsewhere, recomputing lost partitions from lineage (the recorded chain of transformations that produced them) — this is the "Resilient" in "Resilient Distributed Dataset."

Executor cores/slots. Each executor is given a number of cores; each core runs exactly one task at a time. A 2-core executor runs 2 tasks in parallel; a third waits.

                    ┌─────────────────────────────┐
                    │      CLUSTER MANAGER          │
                    │      (spark-master)           │
                    │  Knows which workers have      │
                    │  free CPU/RAM right now.       │
                    └───────────────┬───────────────┘
                     ① "I need 2 executors,
                        2 cores / 2g RAM each"      ② "worker X has room,
                            from Driver                  launch it there"
                                    │                          │
                                    │                          ▼
      ┌──────────────────────┐     │        ┌─────────────────────────────┐
      │        DRIVER          │◄────┘        │   WORKER (spark-worker)      │
      │  (your spark-submit    │              │  ┌─────────────────────┐    │
      │   process — runs        │  ③ sends    │  │     EXECUTOR JVM      │    │
      │   main(), builds        │    tasks    │  │  runs tasks, holds    │    │
      │   the DAG, schedules    │────────────▶│  │  cached partitions,    │    │
      │   tasks, collects       │◄────────────│  │  reports heartbeats    │    │
      │   final results)        │  ④ sends    │  └─────────────────────┘    │
      └──────────────────────┘    results    └─────────────────────────────┘

Project mapping, and an honest correction. Neither spark-master nor spark-worker containers are the driver. spark-master runs the cluster manager daemon (standalone mode's Master). spark-worker runs the Worker daemon, whose job is to launch executor JVM processes when told to. The driver is created when docker compose exec spark-master spark-submit ... runs — a brand-new process, inside the spark-master container.

This is a workaround, not the normal production shape:

Why production doesn't do this: the driver typically runs wherever the job was launched from (an Airflow worker, a CI runner, a notebook server, or a dedicated pod in cluster mode) — no reason to live on the cluster-manager's own machine.
What workaround is used: exec into spark-master just to get a shell already on the same Docker network as spark-worker and minio, so hostnames resolve correctly for both driver and executors.
What production would use instead: a dedicated, disposable driver pod (Kubernetes) or a managed platform's own submission mechanism (Databricks jobs, EMR steps).
Limitation introduced: the spark-master container running both the Master daemon and a driver process at once is an artifact of local dev, not something a real multi-tenant cluster would tolerate.

Interview angle:

Junior: Driver plans and schedules; executors hold data and do the actual compute.
Junior/Mid: Executor crash → driver detects missing heartbeat, reschedules tasks elsewhere, recomputes via lineage. Job usually survives.
Mid: Driver crash → whole application dies. Exactly one driver.
Senior/Principal: "10 nodes, 8 cores/32GB each — how do you decide executor count, cores, memory?" Short answer: never one giant executor per node (GC blast radius, poor fault isolation), never one-core-per-executor (scheduling overhead, duplicated per-JVM overhead). ~4–5 cores/executor is a common starting point, leaving 1 core/~1GB per node for OS/daemons. See the dedicated sizing walkthrough below.
3. DAG, Stages, Tasks, Partitions

Partition — the foundation. A partition is one chunk of the dataset, small enough for one executor core to hold and process. Analogy: photocopying a 1,000-page report into 10 stapled 100-page bundles, one per reviewer.

How partition count is decided when reading a file: spark.sql.files.maxPartitionBytes, default 128 MB. A 40 MB file reads as one partition; a 300 MB file splits into roughly 3.

Project tie-in. data/bronze/clicks/ingestion_date=2026-09-20/clicks.parquet holds 5,000 rows — almost certainly under 1 MB, so it reads as exactly one partition. That's the whole reason the 2-core worker sits mostly idle running explore_clicks.py: one partition means one task, means one core doing anything.

Hands-on check:

python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("partition-check").getOrCreate()

df = spark.read.parquet("data/bronze/clicks/ingestion_date=2026-09-20/clicks.parquet")
print("partitions:", df.rdd.getNumPartitions())

df2 = df.repartition(4)
print("partitions after repartition(4):", df2.rdd.getNumPartitions())

df2.count()
spark.stop()

First print should say 1; second should say 4. While df2.count() runs, http://localhost:4040's "Stages" tab should show a stage with 4 tasks.

Task — the smallest unit of work. "Run this function on this one partition." 4 partitions → 4 tasks; a 2-core executor runs 2 at once, picking up the next as one finishes.

Stage — where tasks get grouped. A stage is a group of tasks needing no data exchange between them. A new stage starts only at a shuffle boundary (full depth in Outcome 2). Analogy: 10 reviewers each summarizing their own bundle is one stage (no cross-talk needed); combining all 10 summaries into one sorted list needs everyone's output gathered first — that gathering is a new stage.

Project tie-in, precisely: .filter(), .select(), .withColumn() in build_valid_clicks() are each row-local — no row needs another row's data. Zero shuffles → exactly one stage for the whole chain.

Job — one action, one job. Every action call creates exactly one job, made of one or more stages. explore_clicks.py calls two actions (.count(), .write.parquet()) → two jobs, each with one stage, each with one task (one partition).

APPLICATION           (one spark-submit run of explore_clicks.py)
   │
   ├── JOB 1            triggered by:  valid_clicks.count()
   │      └── STAGE 0    (no shuffle in this chain → only 1 stage)
   │              └── TASK 0  → runs read+filter+select+withColumn+count on PARTITION 0
   │
   └── JOB 2            triggered by:  valid_clicks.write.parquet(...)
          └── STAGE 0    (again, no shuffle → only 1 stage)
                  └── TASK 0  → re-runs read+filter+select+withColumn, then writes PARTITION 0

Interview angle:

Junior: Partition = a chunk of the dataset small enough for one core; makes parallel processing possible.
Junior/Mid: Task count per stage = partition count being processed.
Mid: Job vs stage — a job is triggered by one action and can contain multiple stages; a new stage starts at each shuffle boundary.
Senior: Two actions on the same un-cached DataFrame → two jobs, second repeats all upstream computation.
Principal: "199 of 200 tasks finish in 2s, one takes 10 minutes" → data skew (Outcome 3 depth); diagnostic instinct is checking per-task duration in the Spark UI's Stages tab for one wildly larger input size.
4. Transformations vs Actions

Analogy: an online shopping cart. Adding/removing items, applying a coupon — none of these charge your card. Only "Place Order" actually charges and ships. Adding/removing = transformations. Placing the order = an action.

Precise definitions. A transformation takes a DataFrame, returns a new DataFrame describing one more plan step, and touches no actual data when called: .filter(), .select(), .withColumn(), .join(), .groupBy(), .orderBy(), .distinct(), .repartition(). An action actually triggers execution and produces a real result: .count(), .collect(), .show(), .take(n), .write.parquet(...), .foreach(...).

Immutability. DataFrames are immutable. clicks_df.filter(...) does not modify clicks_df — it returns a new object. Forgetting to capture the return value silently discards the transformation:

python
clicks_df.filter(F.col("id").isNotNull())          # WRONG: return value discarded
valid = clicks_df.filter(F.col("id").isNotNull())  # correct

Project classification (explore_clicks.py):

Call	Transformation or Action	Why
spark.read.parquet(...)	Transformation	Describes "read from here"; doesn't read yet
.filter(F.col("id").isNotNull())	Transformation	Describes a filter; doesn't apply it yet
.select(...)	Transformation	Describes a projection
.withColumn(...)	Transformation	Describes a column change
.count()	Action	Actually runs everything above, returns an integer to the driver
.write.mode("overwrite").parquet(...)	Action	Actually runs everything above, writes real files to MinIO

.collect() and driver OOM. .collect() pulls every row back to the driver as an in-memory Python list. The driver is one process with a much smaller footprint than the cluster combined — .collect() on 500 million rows will very likely OOM the driver. One of the most common real production incidents in Spark, and a frequent interview question because it's easy to write by accident.

Interview angle:

Junior: Transformations build the plan and return immediately; actions run it.
Junior/Mid: .collect() is dangerous on large DataFrames — pulls all rows into the driver's single-process memory.
Mid: DataFrames are immutable; every transformation returns a new object; immutability is also what makes it safe for Spark to recompute a lost partition from lineage.
Senior/Principal: Inspect a huge DataFrame safely with .show(n)/.take(n)/.sample(), or write the result to storage and inspect it there — never .collect() it whole.
5. Lazy Evaluation

Analogy: a road-trip GPS. You type in several stops; the app doesn't recalculate after each one. It waits until you press "Start," then looks at all stops together and computes one optimized route — possibly reordering things it would never have chosen committing route-by-route as you typed. That's lazy evaluation: defer execution until the answer is actually needed, so the whole picture can inform a smarter plan.

Catalyst Optimizer, step by step:

Your DataFrame code (.filter, .select, .withColumn, ...)
        │
        ▼
1. UNRESOLVED LOGICAL PLAN
   column/table names referenced, not yet checked against the real schema
        │
        ▼
2. ANALYSIS → ANALYZED LOGICAL PLAN
   Spark resolves every column/table reference and checks types -- a typo'd
   column name surfaces here, which only happens when an action runs it
        │
        ▼
3. LOGICAL OPTIMIZATION → OPTIMIZED LOGICAL PLAN
   Rule-based rewrites: predicate pushdown, column pruning, constant folding
        │
        ▼
4. PHYSICAL PLANNING → PHYSICAL PLAN(S)
   One or more concrete execution strategies (e.g. join algorithm --
   Outcome 3), cheapest picked via a cost model
        │
        ▼
5. CODE GENERATION (Tungsten)
   Compiled into JVM bytecode, combining operations into tight loops
   ("whole-stage code generation")
        │
        ▼
   ONLY NOW does anything actually run -- and only because an ACTION was called

Predicate pushdown, concretely. spark.read.parquet(bronze_path).filter(F.col("device_type") == "mobile") — without pushdown, Spark reads every row then discards non-mobile ones; with pushdown, the Parquet reader itself is told to skip rows (and, via per-row-group min/max statistics, sometimes skip whole chunks without opening them). Same core idea reappears, at bigger scale, in Outcome 7 (Trino) and Outcome 8 (query performance).

See it yourself: valid_clicks.explain(True) — look for four labeled sections: == Parsed Logical Plan ==, == Analyzed Logical Plan ==, == Optimized Logical Plan ==, == Physical Plan ==, matching the phases above. Compare whether Filter appears before or after Project in the Optimized plan versus the Parsed plan.

Interview angle:

Junior: Transformations don't execute immediately; Spark waits for an action, then runs the whole chain at once.
Mid: Laziness lets the optimizer see the entire plan before running anything, enabling predicate pushdown/column pruning that per-line eager execution couldn't do.
Mid/Senior: A column-name typo surfaces at Analysis time — only when an action runs — potentially far from where the mistake was written.
Senior/Principal: Catalyst picks between physical plans (e.g. join strategies) via cost-based optimization — the on-ramp to Outcome 3's broadcast-join material.
6. Extra foundations needed before Outcome 2
RDD vs DataFrame vs Dataset. RDD: Spark's original, lowest-level abstraction — a distributed collection of opaque objects, no schema, not optimizable by Catalyst. DataFrame: schema-aware, built on RDDs, what Catalyst can reason about. Dataset: typed DataFrame (Scala/Java only). This project uses the DataFrame API throughout.
SparkContext vs SparkSession. Pre-2.0 Spark needed a SparkContext (RDDs) plus a separate SQLContext/HiveContext (DataFrames/SQL). SparkSession unifies all of these — the only entry point this project uses.
Cluster manager types. Standalone (this project's spark-master/spark-worker) is Spark's own simple built-in manager, rarely used in real production at scale. Real alternatives: YARN (Hadoop clusters), Kubernetes (increasingly the cloud-native default — what the official image's driver/executor entrypoint handling is actually built for), Mesos (largely deprecated). Managed platforms (Databricks, EMR, Dataproc) sit on top of YARN or Kubernetes with most of this hidden. Standalone here is purely for local-Docker-Compose simplicity, not a production recommendation.
Narrow vs wide, previewed. Narrow (.filter(), .select(), .withColumn()): each output partition depends on exactly one input partition, no data movement. Wide (.groupBy(), .join(), .distinct(), .orderBy()): correct output requires comparing rows that might live on different machines, so data must physically move across the network — a shuffle, the single most expensive kind of Spark operation. Full mechanics: Outcome 2.
Practice (from this Deep Dive)
Run the local[2] partition-count script above — confirm the count goes from 1 to 4 after repartition(4), and check the Spark UI (http://localhost:4040) shows 4 tasks for that job.
Add .explain(True) after building valid_clicks in a scratch copy of the script (local mode, pointed at the local data/bronze/... path instead of s3a://...), and read all four plan sections. Note one thing that matched or surprised relative to the Catalyst phases above.
Follow-up — Designing Executor Count, Cores, and Memory

(A common Spark system-design interview question: given a cluster and a data volume, how do you actually size executors? Worked with concrete assumed numbers, clearly flagged as an illustrative scenario, not a measured one.)

The generic idea

Analogy: hiring movers for a warehouse job. Each worker carries some number of boxes at once (cores). Each worker also needs a personal bag for tools (memory overhead — space to keep the worker functioning, not for boxes). Too few workers each carrying too many boxes at once → contention over shared tools, and a dropped bag (GC pause) stops many boxes at once. Too many workers each carrying one box → more time coordinating than moving boxes. A giant bag but only three workers hired → most of the warehouse floor space sits unused.

The assumed scenario
CLUSTER:  10 worker nodes, 16 cores/node, 64 GB RAM/node
          → 160 total cores, 640 GB total RAM (raw)
DATA:     500 GB of Snappy-compressed Parquet
JOB:      narrow filtering/selection, then a later groupBy (shuffle)
TENANCY:  dedicated to this job
MODE:     cluster mode (driver runs on the cluster)
Step 1 — reserve resources for OS/daemons

Rule of thumb: 1 core + 1 GB RAM per node, for the OS and cluster daemons (NodeManager, DataNode on YARN).

Per node:  16 cores, 64 GB RAM   (raw)
Reserve:    1 core,   1 GB RAM
──────────────────────────────
Available: 15 cores, 63 GB RAM
Step 2 — cores per executor

Sweet spot: ~4–5 cores/executor. Two reasons: (1) all cores in one executor share one JVM heap — a GC pause stalls every task in that executor at once, so 16 cores in one executor means 16 stalled tasks per pause vs. 5; (2) tasks in one executor share things like the S3/HDFS client's connection pool — too many concurrent tasks causes contention there.

Too few cores per executor is also bad: more small JVMs, each duplicating fixed overhead (broadcast variables copied once per executor), more shuffle-service connections cluster-wide.

Decision: 5 cores/executor.

Step 3 — executors per node
executors_per_node = floor(15 / 5) = 3

3 × 5 = 15, using every available core cleanly.

Step 4 — memory per executor
raw_mem_per_executor = 63 GB / 3 = 21 GB

memory_overhead = max(384 MB, 0.10 × 21 GB) = 2.1 GB

spark.executor.memory = 21 GB − 2.1 GB ≈ 19 GB

The cluster manager reserves the full 21 GB as a hard container limit; heap + overhead must fit inside it together.

Step 5 — total executors, and the driver's slot
executors_per_node × num_nodes = 3 × 10 = 30 slots

Cluster mode: the driver runs as a container on a worker node too, needing its own slot — roughly one executor's worth.

Total working executors = 30 − 1 = 29

(In client mode — like this project's docker compose exec spark-master spark-submit ... — the driver runs outside this pool entirely, and all 30 stay usable.)

Step 6 — the final spark-submit config
bash
spark-submit \
  --deploy-mode cluster \
  --num-executors 29 \
  --executor-cores 5 \
  --executor-memory 19g \
  --conf spark.executor.memoryOverhead=2g \
  --driver-cores 5 \
  --driver-memory 19g \
  your_job.py
Step 7 — sanity-check against the actual data

Check 1: partition count vs. core count.

partitions ≈ 500 GB / 128 MB ≈ 4,000 tasks (Stage 1)
total_cores_working = 29 × 5 = 145
tasks_per_core = 4,000 / 145 ≈ 27.6 "waves"

28 waves is fine for a one-time 500 GB batch job — the real check is whether each task does meaningful work: ~125 MB/task is healthy relative to per-task scheduling overhead (a few hundred ms).

Check 2: per-task memory footprint vs. executor heap.

per_partition_on_disk   ≈ 125 MB
per_partition_in_memory ≈ 125 MB × 3 ≈ 375 MB   (illustrative decompression/deserialization multiplier)
concurrent tasks/executor = 5
minimum working set/executor ≈ 5 × 375 MB ≈ 1.9 GB

Against 19 GB of heap, 1.9 GB minimum working set leaves large headroom for shuffle buffers and JVM overhead — this configuration passes. If the working set neared the heap size instead, the fix is fewer cores per executor (less concurrency fighting for the same heap), not blindly more memory.

ONE WORKER NODE — 16 cores, 64 GB RAM (raw)
┌──────────────────────────────────────────────────────────────────┐
│ OS + YARN daemons:  1 core, 1 GB          (reserved, Step 1)      │
├──────────────────────────────────────────────────────────────────┤
│ Executor 1: 5 cores │ heap 19 GB │ overhead 2 GB │ (21 GB total)  │
├──────────────────────────────────────────────────────────────────┤
│ Executor 2: 5 cores │ heap 19 GB │ overhead 2 GB │ (21 GB total)  │
├──────────────────────────────────────────────────────────────────┤
│ Executor 3: 5 cores │ heap 19 GB │ overhead 2 GB │ (21 GB total)  │
└──────────────────────────────────────────────────────────────────┘
   15 cores used, 63 GB used  →  matches Step 1's "available" exactly
Nuances worth naming
Dynamic allocation (spark.dynamicAllocation.enabled=true, minExecutors/maxExecutors) lets Spark request/release executors as workload changes — matters most on a shared, multi-tenant cluster, where a fixed reservation of 29 idle executors between stages steals capacity from other teams.
Kubernetes vs YARN wording differs (resource requests/limits vs. container reservations), but the underlying reasoning (cores-per-executor sweet spot, memory-overhead-as-a-fraction) is the same.
Adaptive Query Execution (AQE, Spark 3.2+, on by default) can auto-coalesce small shuffle partitions and react to skew at runtime — it optimizes within the resources given, it can't invent cores or memory that weren't allocated. (Preview only; Outcome 2/3 cover AQE's shuffle-partition coalescing in depth.)
This is a starting point, not a final answer — real sizing gets validated and iterated against the Spark UI: shuffle spill to disk (memory too small), GC time relative to task time (too many cores per heap), or a few outlier-duration tasks (skew, not a sizing problem — Outcome 3).
Interview angle
Junior: Why not one giant executor with all the node's cores/memory? — One JVM, one GC pause stalls everything in it; single point of contention for shared resources.
Mid: Why reserve 1 core/1GB per node first? — The OS/cluster daemons need guaranteed resources or the node itself becomes unstable.
Mid/Senior: Walk through the memory overhead calculation. — max(384MB, 10% of executor memory), subtracted from the node's per-executor slice, since the cluster manager enforces heap+overhead as one hard container limit.
Senior/Principal: How do you check whether your executor/core sizing is actually a good fit for the data, not just formula-compliant? — Compute expected partitions from data size, compare to total cores for task "waves," estimate per-task in-memory footprint against heap size.
Principal: When would you deliberately deviate from ~5 cores/executor? — Shuffle-heavy jobs with large per-record accumulator state might want fewer cores/more memory each to cut GC contention further; many small, cheap narrow-transformation jobs tolerate more cores/executor with less GC risk. Always verify against the Spark UI, not just the rule.
Practice: size a second scenario yourself
CLUSTER: 5 worker nodes, 32 cores/node, 128 GB RAM/node
DATA:    2 TB of Parquet
MODE:    client mode (driver runs outside the cluster — no slot reserved for it)

Work through: reserved cores/RAM per node, cores/executor (check whether 5 divides cleanly; adjust if not), executors/node, total executors, raw memory/executor, memory overhead, final spark.executor.memory. Then check partition count from 2 TB at ~128 MB target, and tasks-per-core against total cores.

<details> <summary>One valid worked answer (not the only defensible one)</summary>

Reserve 1 core/1GB per node → 31 cores, 127 GB available. 31 doesn't divide cleanly by 5; a clean choice is 6 executors/node at 5 cores each (30 cores used, 1 idle). 6 × 5 nodes = 30 total executors (all usable — client mode, no driver slot needed). Memory: 127 GB ÷ 6 ≈ 21.2 GB raw; overhead ≈ 2.1 GB; spark.executor.memory ≈ 19 GB. Partitions: 2 TB / 128 MB ≈ 16,384 tasks. Total cores = 30 × 5 = 150. Tasks/core ≈ 109 waves — a lot, but plausible for 2 TB; per-task size (~128 MB) is still healthy, so this is a longer job, not automatically a broken one.

</details>
Honest caveat

The project's own docker-compose cluster is 1 worker, 2 cores, 2 GB — this sizing exercise doesn't meaningfully apply at that scale (no real "reserve 1 core out of 2" decision to make). This kind of math becomes real against an actual multi-node cluster (EMR/Databricks/Kubernetes), outside this project's local Compose scope, but is exactly the sort of thing asked as a design question more often than it's tuned from scratch on day one of a real job.

Follow-up — Factoring In Time (SLA-Driven Sizing, Amdahl's Law)

(The executor-sizing walkthrough above answered "is this cluster big enough" — not "how fast will it finish" or "what size cluster do I need to finish by X." This follow-up adds that missing dimension.)

Work, rate, and time

Same relationship as distance = speed × time, relabeled:

total_work (core-seconds)  =  concurrent_cores  ×  wall_clock_time

required_concurrent_cores  =  total_work (core-seconds)  /  target_wall_clock_time

"Total work" = number_of_tasks × average_task_duration_on_one_core — roughly fixed for a given job/dataset regardless of how many cores you spread it across.

Applying it to the 500 GB example
partitions (tasks)          ≈ 4,000
per-task data size           ≈ 125 MB
assumed per-core throughput  ≈ 20 MB/s   (illustrative assumption, not a measured number)

avg_task_duration = 125 MB / 20 MB/s ≈ 6.25 s
total_work = 4,000 × 6.25 s = 25,000 core-seconds

Question A — 10-minute (600s) SLA for this stage:

required_cores = 25,000 / 600 ≈ 42

Far less than the 145 cores provisioned earlier — if 10 minutes is genuinely all that's needed, last section's sizing was over-provisioned for this stage.

Question B — 30-second SLA:

required_cores = 25,000 / 30 ≈ 833

Roughly 5× the entire 10-node/160-core cluster. Hitting this would mean scaling out to ~55 nodes of the same shape.

The non-obvious move at this point: ask whether 500 GB needs reprocessing every run at all, instead of just buying more hardware. This is exactly why Phase 1 has both extract_full.py (reprocesses everything) and extract_incremental.py (only pulls rows since the last watermark, via metadata.get_last_watermark()). If the real daily delta is 2 GB instead of 500 GB, an 833-core problem can become trivial on the original modest cluster. Reducing the work is usually cheaper and more sustainable than growing the cluster to brute-force the same work faster.

Amdahl's Law — why "just add cores" has a ceiling

Analogy. Baking a cake with 10 friends: mixing batter splits across all 10 (faster with more people), but only one oven exists, and baking takes 40 minutes regardless of how many friends help. No number of friends makes the cake ready faster than 40 minutes, because that part isn't parallelizable at all.

speedup(N cores) = 1 / ( (1 - P) + P/N )

P = parallelizable fraction. As N → ∞, speedup approaches a hard ceiling: 1 / (1 - P).

Concrete numbers. 1,000 seconds on one core, P = 0.9 (90% parallelizable, 10% inherently serial — e.g. driver-side final commit): max speedup = 1 / 0.1 = 10×. Floor on wall-clock time: 1,000s / 10 = 100 seconds, no matter how large the cluster.

Time vs. cores added, with a 10% serial floor
1000s ┤●
      │  ●
 500s ┤     ●
      │         ●
 250s ┤              ●
 100s ┤                    ●───●───●───●───●───●   ← floor: never goes below this
      │                                              no matter how many cores added
   0s └──────────────────────────────────────────────
      1     2     4     8    16    32    64   128  cores

In Spark, real examples of the "serial 10%": the driver planning the job before any task starts, a shuffle's synchronization barrier (the next stage can't start until every task of the previous one finishes — one straggler holds up everyone), single-partition final write/commit steps.

Real friction beyond the pure formula
Per-task scheduling overhead — commonly a few hundred ms per task to serialize/schedule/collect. Fine when tasks do 6+ seconds of real work; dominant, and counterproductive, when partitions shrink to a handful of rows each.
Stragglers/skew — one oversized or expensive partition holds up the whole stage at the shuffle barrier (Outcome 3 depth). More tasks than cores (the "2–4× tasks-per-core" heuristic) gives Spark room to reassign work — but too many tiny tasks reintroduces the scheduling-overhead problem above.
Executor startup/acquisition latency — getting a new executor running (container placement, JVM boot, registration with the driver) can be tens of seconds cold, or minutes on an autoscaling cloud cluster spinning up new machines. For a tight SLA (e.g. 30s), this alone can exceed the whole budget — why tight-SLA production systems often use pre-warmed executor pools (Databricks pools, or a cluster kept running) instead of provisioning from zero per job.
Revised sizing procedure, with time as an explicit input

Six inputs, not five:

Cluster shape
Data volume
Job shape (stages, shuffles)
Multi-tenancy
Deploy mode
Time SLA — is there a required finish time?

Without #6: size executors sensibly into the given cluster, sanity-check against data volume (the earlier walkthrough's direction). With #6: compute total_core_seconds, divide by target time for required_cores, check against Amdahl's-Law-adjusted expectations, then decide whether to scale the cluster, reduce the data volume, or both.

Interview angle
Junior: Doubling executors doesn't automatically halve job time — depends on available parallel work and the inherently-serial portion.
Mid: Amdahl's Law caps max speedup at 1/(1-P); a real serial bottleneck (shuffle barrier, single-partition commit) can't be sped past that ceiling by adding cores.
Mid/Senior: "40 minutes → must be 10" — check available parallelism first (partitions vs. cores), check for stragglers/skew, check whether a shuffle-heavy stage is the real bottleneck, then consider more hardware; separately consider shrinking the data volume (incremental vs. full).
Principal: When does adding executors stop helping or start hurting? — Past the point where the serial/shuffle-bound portion dominates (Amdahl), where per-task overhead exceeds real task work, or where more concurrent readers start overwhelming a shared upstream resource (a real concern for this project's own object_store.py under enough concurrent MinIO calls).
Principal: Scale the cluster or reduce the data processed, to hit a tight SLA? — Both are valid; reducing data volume (filters, incremental processing, partition pruning) is usually cheaper and more sustainable and reduces load on upstream systems too; scaling hardware is often the faster short-term fix. Check the workload-reduction lever first.
Practice — observe the overhead effect on the project's own tiny dataset
python
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[4]").appName("time-factor-experiment").getOrCreate()

df = spark.read.parquet("data/bronze/clicks/ingestion_date=2026-09-20/clicks.parquet")

for n in [1, 4, 50, 500]:
    reparted = df.repartition(n)
    start = time.time()
    reparted.count()
    elapsed = time.time() - start
    print(f"partitions={n:>4}  elapsed={elapsed:.3f}s")

spark.stop()

Watch the trend across the four partition counts. Check specifically whether 500 partitions (~10 rows each) comes out slower than 4 — if so, that's per-task scheduling overhead outweighing real work per task, the same principle behind the 833-cores example, directly observable in under a minute.

Troubleshooting Log — ModuleNotFoundError: No module named 'pydantic_settings'

(A real error hit running make explore-clicks for real, worked through and fixed. Kept here as an honest record, not smoothed over — this exact class of bug (host dev environment vs. cluster image parity) is common in real Spark deployments and worth having a worked example of.)

What happened
make explore-clicks
...
File "/opt/spark/work-dir/phase2-spark/src/jobs/explore_clicks.py", line 19, in <module>
    from phase2_spark.config import get_phase2_settings
  File "/opt/spark/work-dir/phase2-spark/src/phase2_spark/config.py", line 5, in <module>
    from pydantic_settings import BaseSettings, SettingsConfigDict
ModuleNotFoundError: No module named 'pydantic_settings'
Root cause

Analogy. Mailing a letter full of instructions to a friend's office computer, asking them to run it there. Their computer doesn't have any of the apps installed on your own laptop — only whatever came factory-installed. The instructions fail the moment they need an app that exists only on your machine.

Project-specific. make install-transform installs pydantic-settings, boto3, pandas, etc. into the host's .venv-analytics. spark-submit runs inside the spark-master container (via docker compose exec), built from the stock spark:3.5.9-python3 image — which only ships Python + PySpark's own runtime deps (py4j, etc.), nothing project-specific. The driver process (inside that container) hits from pydantic_settings import BaseSettings in config.py, and there's genuinely nothing there to import.

One thing correctly solved already, before this error: adding -e PYTHONPATH=/opt/spark/work-dir/phase2-spark/src to docker compose exec — that's what let Python find the phase2_spark package at all (confirmed by the traceback locating config.py successfully). PYTHONPATH only tells Python where to look; it does nothing to install a package that was never pip installed anywhere. Two separate problems.

The fix — a custom Spark image with dependencies baked in

Production-realism disclosure:

Why the bare official image was never enough: any real Spark deployment needs third-party Python packages available to the driver and executors; the stock Apache image intentionally ships nothing project-specific.
What was done: a small custom image, FROM spark:3.5.9-python3, with one extra pip install layer. Both spark-master and spark-worker build from the same Dockerfile, so driver and every executor have an identical Python environment.
What real production does beyond this: publishes the custom image to a private registry (ECR/GCR/Artifact Registry) with a pinned tag/digest, rebuilt through CI whenever dependencies change — not relying on docker compose build picking up a local Dockerfile edit on one developer's machine.
Limitation this local version introduces: if phase2-spark's dependencies change later and docker compose build isn't rerun, containers silently keep running the old image — no CI check enforces a rebuild here.
NEW — phase2-spark/docker/Dockerfile
dockerfile
# The official spark:3.5.9-python3 image only ships Python + PySpark's own
# runtime deps (py4j, etc). It does NOT include phase2_spark's own
# third-party dependencies -- those were only ever installed into the HOST
# machine's .venv-analytics by `make install-transform`, a completely
# separate Python environment from the one spark-submit actually runs
# inside. See "environment parity between host and cluster image" above.
FROM spark:3.5.9-python3

# Pin these to match whatever your own .venv-analytics actually resolved --
# run `pip show pydantic-settings pydantic` there and compare before
# building. A silent version drift between host dependency versions and the
# cluster image's versions is the same class of bug as the PYSPARK_PYTHON
# interpreter mismatch noted in Environment above -- one level up, at the
# package version instead of the interpreter version.
RUN pip install --no-cache-dir "pydantic-settings==2.15.0" "pydantic==2.13.5"

# Baked in at the image level (not passed as a `docker compose exec -e`
# flag) so BOTH the driver (spark-submit, exec'd into spark-master) and
# every executor (launched fresh by the Worker daemon inside spark-worker's
# own container, as a child process of that daemon) inherit it
# automatically. A `docker compose exec -e` flag only sets the env var for
# the one process spark-submit becomes; it never reaches the separate
# executor JVMs spark-worker spawns.
ENV PYTHONPATH=/opt/spark/work-dir/phase2-spark/src
MODIFIED — docker-compose.yml (spark-master/spark-worker, replacing the image: lines from Implementation above)
yaml
  spark-master:
    build:
      context: .
      dockerfile: phase2-spark/docker/Dockerfile
    container_name: analytics-platform-spark-master
    restart: unless-stopped
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
    ports:
      - "7077:7077"
      - "8090:8080"
    volumes:
      - .:/opt/spark/work-dir:ro

  spark-worker:
    build:
      context: .
      dockerfile: phase2-spark/docker/Dockerfile
    container_name: analytics-platform-spark-worker
    restart: unless-stopped
    depends_on:
      - spark-master
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]
    ports:
      - "8091:8081"
    environment:
      SPARK_WORKER_CORES: "2"
      SPARK_WORKER_MEMORY: "2g"
    volumes:
      - .:/opt/spark/work-dir:ro

The volumes: line is what makes /opt/spark/work-dir/phase2-spark/... exist inside the container at all — the repo root bind-mounted at /opt/spark/work-dir (the base image's own WORKDIR). A live bind-mount of the working directory is a dev-only convenience; production would COPY code into the image at build time, or ship it via --py-files/--archives at submit time, rather than depend on a developer's local filesystem being present unchanged.

MODIFIED — Makefile (explore-clicks target, simplified now that PYTHONPATH is baked into the image)
makefile
explore-clicks:
	docker compose exec spark-master \
		/opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.pyspark.python=python3 \
		phase2-spark/src/jobs/explore_clicks.py

(Path adjusted to match the actual on-disk layout used — phase2-spark/src/jobs/explore_clicks.py rather than nested under phase2_spark/jobs/; either works as a bare spark-submit entrypoint path, since only the phase2_spark package itself needs to be importable via PYTHONPATH, not the entrypoint script.)

Run & observe
bash
# check host-resolved versions first, adjust the Dockerfile's pip install
# line above if they differ from 2.15.0 / 2.13.5
pip show pydantic-settings pydantic | grep -E "Name|Version"

# rebuild both images with the new Dockerfile
docker compose build spark-master spark-worker

# bring the rebuilt cluster up
docker compose up -d

# rerun the job
make explore-clicks

Expected: the ModuleNotFoundError is gone, since pydantic-settings now exists in the same Python environment the driver runs in. If it fails again, check first whether the pinned version in the Dockerfile actually satisfies config.py's imports.
