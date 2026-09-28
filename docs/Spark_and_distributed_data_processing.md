OUTCOME 1 — SPARK EXECUTION FUNDAMENTALS
1. CONCEPT

Everything else in Phase 2 — shuffles, joins, caching, quarantine logic, data quality checks — is built on top of four ideas, and if any one of them is fuzzy, every later debugging session gets harder:

Driver vs Executors. When you run a Spark job, one JVM process (the driver) runs your main() code, builds up a plan, and asks a cluster manager for workers. Separate JVM processes (executors) actually hold data partitions in memory and run the compute. The driver never processes rows itself — it only ever schedules work. This matters immediately for your pipeline: pd.read_sql_table in extract_full.py (Phase 1) pulls the entire result into the driver's own memory as a single pandas DataFrame — there's no "driver vs executor" split at all. Phase 2 is the first place in this project where that split exists, and it's the reason a 50M-row table that would OOM a Phase-1-style pandas extraction can be processed by Spark without OOMing the driver: the rows never all live in one process at once.

DAG, stages, tasks, partitions. A Spark job is a directed acyclic graph (DAG) of operations. Spark doesn't run each line of your code as you write it — it builds the whole DAG first, then compiles it into stages (groups of operations that can run without moving data between machines), and each stage is split into tasks — one task per partition of the data. A partition is simply a chunk of the dataset that fits in one executor core's memory and is processed independently. If your Bronze Parquet file is read as 4 partitions and you have 4 executor cores, you get 4 tasks running in parallel, one per partition.

Transformations vs actions. Spark's DataFrame API has two kinds of operations. Transformations (.filter(), .select(), .withColumn(), .join()) describe a new DataFrame in terms of an existing one — Spark records this as a step in the DAG and does no actual computation. Actions (.count(), .collect(), .write.parquet(), .show()) are the only operations that trigger real execution — they force Spark to actually run the DAG built up to that point, producing a concrete result.

Lazy evaluation is the reason that split exists: nothing runs until an action is called. This isn't an implementation detail you can ignore — it's the reason a stack trace from a .write.parquet() call can point at a .filter() you wrote 10 lines earlier: the filter didn't fail when you wrote it, because it didn't run when you wrote it.

2. PROJECT EXAMPLE

Your Phase 1 pipeline already produces real Bronze data that's perfect for this: ingestion/src/url_shortener_analytics/object_store.py's build_bronze_key() wrote a full-load snapshot of the clicks table to a deterministic key, and I confirmed it's sitting on disk right now:

data/bronze/clicks/ingestion_date=2026-09-20/clicks.parquet

(In your real MinIO bucket this is the object key bronze/clicks/ingestion_date=2026-09-20/clicks.parquet in the analytics-lake bucket — data/bronze/ on disk is just this sandbox's local mirror of what MinIO actually holds.)

Outcome 1's job reads exactly this file with Spark, applies two narrow transformations, and calls two actions — enough to see a DAG, stages, tasks, and the lazy-vs-eager boundary directly, without yet touching shuffle (that's Outcome 2).

I'm deliberately reading only the ingestion_date=* (full-load) partitions in this outcome, not the incremental/watermark_start=.../watermark_end=... partitions that also exist under bronze/clicks/. Reading both at once with a single spark.read.parquet("bronze/clicks/") call hits a real bug: Spark's Hive-style partition discovery tries to infer one consistent partition-column schema for everything under a directory, and ingestion_date= vs watermark_start=/watermark_end= are two different schemes, which throws AssertionError: Conflicting directory structures detected. That's a real, already-discovered issue in this exact Bronze layout — it's Outcome 4/5 material (how do you actually reconcile full-load and incremental batches into one Silver table), not Outcome 1's. Here we sidestep it honestly with a glob scoped to only the full-load partition:

bronze/clicks/ingestion_date=*/clicks.parquet
3. ENVIRONMENT

What I verified directly, in this sandbox, before writing anything below:

PySpark is 3.5.9, already installed in .venv (Phase 1's transform optional-dependency group already pulled it in).
The official Docker Hub spark image has a tag 3.5.9-python3 that matches this installed version exactly.
I fetched the actual Apache spark-docker repo source for that image (not from memory): SPARK_HOME=/opt/spark, ENTRYPOINT ["/opt/entrypoint.sh"], and the entrypoint script's routing logic is a case statement — driver and executor are handled specially (Kubernetes-oriented), and anything else falls through to exec "$@". That means a docker-compose command: override like ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"] runs exactly as given — this is why the compose services below invoke spark-class directly rather than relying on any wrapper script the image doesn't actually provide for standalone mode.
I hit a genuine environment bug while self-testing the transformation logic below, and fixed it before showing you anything: PySpark's worker subprocess was resolving a bare python3 on $PATH to system Python 3.11, while the driver (this sandbox's .venv) is Python 3.12 — a real PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON mismatch that Spark refuses to run under (PySparkRuntimeError: [PYTHON_VERSION_MISMATCH]). Fix: explicitly export PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to the exact same interpreter path. Once I did that, the test genuinely passed. You'll hit an equivalent version of this the first time you run Spark from your own machine if your python3 on PATH isn't the same interpreter you installed pyspark into — the fix is identical.

What I have NOT run in this sandbox (no Docker daemon here): the actual spark-master/spark-worker containers, a real spark-submit against them, or a real S3A read from MinIO. Those commands are correct against verified facts (image tag, entrypoint behavior, Hadoop/AWS-SDK jar versions from Phase 1's earlier research), but you are the one who will actually execute them and see real Spark UI output — that's the whole point of RUN & OBSERVE below, and I won't pretend to have seen a Spark UI I don't have access to.

4. DESIGN

Where this lives, and why it's structured this way:

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

A separate Phase2Settings, not Phase 1's Settings. Phase 1's config.py (ingestion/src/url_shortener_analytics/config.py) has fields like database_url that Spark jobs have no business depending on, and reusing it would silently couple Phase 2's config lifecycle to Phase 1's. Phase2Settings reads the same .env file (so MINIO_ENDPOINT, MINIO_ACCESS_KEY etc. aren't duplicated as literals) but is its own BaseSettings subclass, so Phase 2 can add Spark-only fields (spark_master_url) without touching Phase 1's class at all. This is "common infrastructure reused" (the .env file, the pydantic-settings pattern) without "Phase 1 code reused" (the Settings class itself).
spark_session.py is a single function, build_spark_session(), rather than inline SparkSession.builder calls scattered across jobs. Every job in Phase 2 will need identical S3A wiring; centralizing it means Outcome 2 through 10's jobs don't re-derive it, and a config mistake gets fixed in one place.
explore_clicks.py has a pure function (build_valid_clicks) separated from I/O (main). This is the same shape Phase 1 already uses (e.g. extract_full() is pure-ish relative to run_full_load()'s orchestration) — and it's what makes the unit test below possible without spinning up MinIO or a Spark cluster: build_valid_clicks takes a DataFrame and returns a DataFrame, so a test can hand it a tiny in-memory DataFrame under local[1] and assert on the result with zero cluster, zero S3A, zero network.
Docker Compose gets two new services (spark-master, spark-worker), added to the existing docker-compose.yml rather than a second compose file. This is genuinely shared infrastructure — one compose project, one make up — not Phase 2 "implementation" logic, so extending it fits the phase-isolation rule's own carve-out for common infra. The services join the same default Compose network Postgres and MinIO already use, so spark://spark-master:7077 and http://minio:9000 both resolve by Docker's built-in service-name DNS.
pyproject.toml's existing transform extra (pyspark==3.5.9) is reused as-is, and only the packaging metadata is extended to also discover phase2_spark. The extra itself is a version pin, not logic — duplicating it under a second name would violate "do NOT duplicate common infrastructure unnecessarily" for no benefit.
5. IMPLEMENTATION
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
Phase 1's class staying stable -- see docs Outcome 1, Design Decision.
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
    caveat applies: see Phase 1 Component 1's lru_cache discussion if you
    need to reload settings mid-process (e.g. in a long-lived notebook)."""
    return Phase2Settings()
NEW — phase2-spark/src/phase2_spark/spark_session.py
python
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

I ran this exact test in this sandbox (local[1], with the PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON fix applied) — it genuinely passed:

test_explore_clicks.py::test_build_valid_clicks_drops_null_ids_and_lowercases_device_type PASSED
1 passed in 8.42s
MODIFIED — docker-compose.yml (add two services; nothing existing changes)
yaml
  # Phase 2: a minimal standalone Spark cluster (one master, one worker).
  # The official image's entrypoint only special-cases "driver"/"executor"
  # (Kubernetes-oriented); anything else -- including spark-class here --
  # is exec'd through unchanged, confirmed against apache/spark-docker's
  # own entrypoint.sh.template.
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
makefile
# Phase 2 (fresh module) -- separate from test-transform, which runs the
# earlier transformations/analytics_transform package's own tests.
test-phase2-spark:
	PYTHONPATH=phase2-spark/src python3 -m pytest phase2-spark/tests/unit -v

# Must run *inside* the Docker network so the driver and executors resolve
# spark-master/minio identically -- see Design, Outcome 1.
explore-clicks:
	docker compose exec spark-master \
		/opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.pyspark.python=python3 \
		phase2-spark/src/phase2_spark/jobs/explore_clicks.py
6. CODE WALKTHROUGH

The one thing worth pausing on that isn't obvious syntax: why does main() build clicks_df and valid_clicks before calling any action, and why does calling .count() and then .write.parquet() mean the Parquet file gets read twice?

spark.read.parquet(bronze_path) doesn't read anything. It returns a DataFrame object that's really just a pointer to a not-yet-executed plan: "read Parquet from this path." build_valid_clicks() extends that plan with a filter, a select, and a withColumn — still no execution, still just plan-building. By the time main() reaches row_count = valid_clicks.count(), the full plan is: read → filter → select → withColumn → count. Calling .count() is what turns that plan into an actual Spark job: the driver submits it, gets it split into stages and tasks, executors run it, and a single integer comes back.

The next line, .write.parquet(...), is a second, independent call to the same valid_clicks object. Spark does not remember that it already computed this DataFrame's rows during the .count() call — DataFrames aren't materialized results, they're re-runnable plans. So .write.parquet() triggers a second job that reads the Parquet file from MinIO again and reruns the filter/select/withColumn again. This is not a bug — it's the direct, sometimes-expensive consequence of laziness, and it's exactly what Outcome 3's .cache()/.persist() material exists to fix. I'm deliberately not caching here, so that in RUN & OBSERVE you can see two separate jobs in the Spark UI rather than one — that visible duplication is the point of this outcome's experiment.

7. RUN & OBSERVE

These commands are correct against the verified facts above (image tag, entrypoint pass-through, jar versions), but I have not run them myself — there's no Docker daemon in this sandbox. Run them yourself and actually look at what's described.

bash
# 1. Bring up Postgres, MinIO, and now also spark-master/spark-worker
docker compose up -d

# 2. Confirm the worker actually registered with the master
#    Open http://localhost:8090 in a browser -- you should see 1 worker
#    listed under "Workers", with 2 cores / 2g memory (from
#    SPARK_WORKER_CORES/SPARK_WORKER_MEMORY above).

# 3. Install the phase2_spark package (same pyspark dependency Phase 1's
#    transform extra already declares)
make install-transform

# 4. Run the unit test (no cluster needed -- local[1])
make test-phase2-spark

# 5. Run the real job against the real cluster
make explore-clicks

What to actually look at, not just run:

Spark UI at http://localhost:8090 while make explore-clicks is running (it'll be quick — this dataset is small — so have the tab open and refresh fast, or watch the master's "Running Applications" list). Click into the running/completed application. You should see two separate jobs — one triggered by .count(), one by .write.parquet(). Click into either job and you'll see its stage(s) and, inside a stage, one task per partition of the Bronze file that was read.
Terminal output from make explore-clicks should print valid clicks: <N> followed by wrote output to phase2/explore_clicks_output/. <N> should be less than or equal to the total row count of data/bronze/clicks/ingestion_date=2026-09-20/clicks.parquet — less than, if any seeded click rows happen to have a null id (unlikely given scripts/seed_sample_data.py's generation logic, but the filter exists regardless).
MinIO console at http://localhost:9001 — confirm analytics-lake/phase2/explore_clicks_output/ now has Parquet part-files in it, written by job 2.
8. EXPERIMENT

Comment out the .write.parquet(...) call (job 2) in explore_clicks.py, leaving only the .count() action, and rerun make explore-clicks.

Expected observation: the Spark UI now shows exactly one job instead of two, and MinIO's phase2/explore_clicks_output/ prefix does not get created/updated. This directly demonstrates that each action is an independent trigger of the DAG — removing one action removes exactly one job, with no effect on the other. It's a small experiment, but it's the cheapest possible proof that "the code runs top to bottom" is the wrong mental model for Spark: if it were, removing a line near the bottom wouldn't change how many times the top of the file's read+filter+select actually executed.

9. PRODUCTION VIEW
Correctness: Nothing in this outcome risks correctness — narrow transformations over one immutable Bronze file are deterministic and side-effect-free per run.
Performance: The double-read/double-recompute exposed above (RUN & OBSERVE / EXPERIMENT) is a real performance issue, not just a teaching device — in a large pipeline with several actions over the same DataFrame, this pattern silently multiplies compute cost by the number of actions. Outcome 3 (caching/persistence) is the direct fix; a Principal Engineer reviewing this job today would flag "two actions, no cache" as a first-pass performance smell even at this small scale.
Reliability: This job has zero error handling — no try/except, no retry, no dead-letter path for a malformed Bronze file. That's appropriate for this outcome (the point is DAG mechanics, not resilience) but would not be acceptable as a real production job; Outcome 9 (Phase 2 Production Engineering) is where this gets addressed properly, not here.
Scalability: The job's parallelism today is bounded by how many partitions spark.read.parquet creates when reading one small Parquet file — likely 1, given the file's small size, meaning the "2 executor cores" configured on the worker are mostly idle for this specific job. This is intentional for Outcome 1 (a single, easy-to-reason-about task) but is exactly the small-file/under-parallelization problem Outcome 3 will name explicitly.
Cost: Running a full Spark cluster (even a 1-worker, 2-core one) for a job this small is genuine overkill in isolation — a real team would not stand up Spark to process one small Parquet file. It's justified pedagogically here because the cluster and its execution model are the thing being taught, and this dataset's size is what Phase 1 happens to have actually produced.
10. PRINCIPAL ENGINEER VIEW
"Why does my Spark job seem to hang and then everything happens at once?" is one of the most common junior-to-mid confusion points, and it's a direct, predictable consequence of laziness — not a bug, not slow I/O. A Principal Engineer should be able to explain, in an interview, exactly why Spark chooses to defer execution (it lets the Catalyst optimizer see the whole plan before running anything, enabling optimizations like predicate pushdown that wouldn't be possible if each line executed eagerly and independently).
The driver/executor split is what makes Spark scale past the memory of one machine — and what makes it a distributed system, with distributed-systems failure modes. The moment your data no longer fits in one process's memory (unlike every Phase 1 pipeline, which was pandas-in-one-process), you've also taken on partial failures, network calls between driver and executors, and non-determinism in task scheduling order — trade-offs a single-process pandas job never had to make.
"How many partitions does this DataFrame have, and why?" is a question every senior Spark engineer should be able to answer for any DataFrame they're looking at, because partition count directly determines task count, which directly determines parallelism. For this outcome's job, the honest answer is "however many partitions Spark's Parquet reader decided to create for one small file" — worth knowing precisely (df.rdd.getNumPartitions()), not just assuming.
A DAG is a contract, not a suggestion. Because Spark builds the whole plan before running any of it, a .filter() that references a column that doesn't exist won't raise AnalysisException until you eventually call an action — meaning the stack trace's line number can be far from where the actual mistake was written. Recognizing this early saves a lot of confused debugging later in Phase 2.
11. REMEMBER
Transformations build a plan; actions run it. Nothing executes until an action is called.
The driver schedules; executors compute. The driver never processes your actual data rows.
A DAG compiles into stages (data-movement boundaries) and stages compile into tasks (one per partition).
Calling two actions on the same unc-ached DataFrame reruns the whole upstream plan twice — this is expected behavior, and the reason caching exists.
"It ran" and "it ran once" are different claims — always know how many actions you called before asking why something took as long as it did.
