.PHONY: venv install install-transform up down logs ps db-shell ingest ingest-full create-analytics-schema validate-contracts check-stale-runs reconcile-bronze storage-stats benchmark-parquet benchmark-query-performance benchmark-extraction-time layout-report ingestion-history ingestion-summary pii-report test test-integration test-transform coverage lint fmt seed write-local-bronze-clicks write-local-bronze-clicks-incremental seed-more-clicks transform-silver-clicks

# Load this repo's own .env (git-ignored; copy from .env.example) so every
# recipe below sees real Postgres/MinIO credentials automatically. `make`
# does NOT source .env on its own the way `docker compose` does -- this
# was a real bug (found while running Section 11's Lab 10): without this
# block, `make create-analytics-schema`/`make db-shell` silently fell
# back to a wrong hardcoded default username ("urlshortener", left over
# from an earlier project name) any time no shell in front of `make` had
# already exported these variables by hand, producing a genuine
# `FATAL: role "urlshortener" does not exist` error even with a correct
# .env file sitting right there, unread.
ifneq (,$(wildcard .env))
include .env
export
endif

venv:
	python3.12 -m venv .venv
	@echo "Activate with: source .venv/bin/activate"

install:
	pip install -e ".[dev]"

# Phase 2 only -- pulls in pyspark (a JVM-backed dependency Phase 1 never
# needs). See pyproject.toml's `transform` extra and
# docs/analytics-engineering-guide.md, Phase 2, ADR-016.
install-transform:
	pip install -e ".[dev,transform]"

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

db-shell:
	docker compose exec postgres psql -U $${POSTGRES_USER:-analytics} -d $${POSTGRES_DB:-analytics}

seed:
	python scripts/seed_sample_data.py

ingest:
	python -m url_shortener_analytics.cli run

ingest-full:
	python -m url_shortener_analytics.cli full-load

# Not run by `make up` (unlike sql/source/) -- analytical-layer schema is
# applied explicitly. See docs/analytics-engineering-guide.md Section 11.
create-analytics-schema:
	@for f in sql/analytics/*.sql; do \
		echo "applying $$f"; \
		docker compose exec -T postgres psql -U $${POSTGRES_USER:-analytics} -d $${POSTGRES_DB:-analytics} -f - < $$f; \
	done

validate-contracts:
	python -m url_shortener_analytics.cli validate-contracts

# See docs/analytics-engineering-guide.md Section 16.
check-stale-runs:
	python -m url_shortener_analytics.cli check-stale-runs

# See docs/analytics-engineering-guide.md Section 17.
reconcile-bronze:
	python -m url_shortener_analytics.cli reconcile-bronze

# See docs/analytics-engineering-guide.md Section 18.
storage-stats:
	python -m url_shortener_analytics.cli storage-stats

# See docs/analytics-engineering-guide.md Section 19. Reads the real seeded
# `clicks` table by default; pass ROWS=<n> for a larger synthetic run, e.g.
# `make benchmark-parquet ROWS=200000`.
benchmark-parquet:
	python benchmarks/parquet_vs_csv_vs_json.py $(if $(ROWS),--rows $(ROWS),)

# See docs/analytics-engineering-guide.md Section 26. Temporarily populates
# fact_clicks/dim_url/dim_user with SYNTHETIC rows, times Section 7.1's
# metrics catalog, then cleans up. Needs only Postgres, not MinIO.
benchmark-query-performance:
	PYTHONPATH=ingestion/src python3 benchmarks/query_performance.py $(if $(SCALES),--scales $(SCALES),)

# See docs/analytics-engineering-guide.md Section 26. Temporarily adds
# SYNTHETIC rows to the real source `clicks` table, times extract_full and
# extract_incremental, then cleans up. Needs only Postgres, not MinIO.
benchmark-extraction-time:
	PYTHONPATH=ingestion/src python3 benchmarks/extraction_time.py $(if $(SCALES),--scales $(SCALES),)

# See docs/analytics-engineering-guide.md Section 21. TABLE is required,
# e.g. `make layout-report TABLE=clicks`.
layout-report:
	python -m url_shortener_analytics.cli layout-report --table $(TABLE)

# See docs/analytics-engineering-guide.md Section 22.
ingestion-history:
	python -m url_shortener_analytics.cli ingestion-history

ingestion-summary:
	python -m url_shortener_analytics.cli ingestion-summary

# See docs/analytics-engineering-guide.md Section 23.
pii-report:
	python -m url_shortener_analytics.cli pii-report

test:
	pytest -v

test-integration:
	pytest -v -m integration

# Phase 2 only. Explicit PYTHONPATH + path, same reason
# benchmark-query-performance/benchmark-extraction-time need it: pytest's
# own testpaths (pyproject.toml) stays scoped to ingestion/tests, so plain
# `make test` never requires pyspark to be installed.
test-transform:
	PYTHONPATH=ingestion/src:transformations/src python3 -m pytest transformations/tests/unit -v

# See docs/analytics-engineering-guide.md Section 24. Measured, not
# gated -- no --cov-fail-under threshold yet (Section 24.3).
coverage:
	pytest --cov=url_shortener_analytics --cov-report=term-missing

lint:
	ruff check .

fmt:
	ruff format .

# See docs/analytics-engineering-guide.md, Phase 2, ADR-016 and Section
# 38. Writes a real Bronze `clicks` Parquet snapshot to the local
# filesystem (data/bronze/...) using the real extract_full() function
# against real Postgres -- this sandbox has no real MinIO to write to.
write-local-bronze-clicks:
	PYTHONPATH=ingestion/src python3 scripts/write_local_bronze_clicks.py

# See docs/analytics-engineering-guide.md, Phase 2, Section 39. Inserts a
# real, second batch of clicks rows into Postgres, simulating activity
# that happened after the full-load snapshot above.
seed-more-clicks:
	PYTHONPATH=ingestion/src python3 scripts/seed_more_clicks.py

# See docs/analytics-engineering-guide.md, Phase 2, Section 39. Requires
# write-local-bronze-clicks (for its watermark) and, for a non-empty
# result, seed-more-clicks to have been run first.
write-local-bronze-clicks-incremental:
	PYTHONPATH=ingestion/src python3 scripts/write_local_bronze_clicks_incremental.py

# See docs/analytics-engineering-guide.md, Phase 2, Section 38 and 39.
# Requires write-local-bronze-clicks to have been run at least once
# first; reads every Bronze clicks object it finds (full-load and
# incremental together, per Section 39).
transform-silver-clicks:
	PYTHONPATH=ingestion/src:transformations/src python3 transformations/src/analytics_transform/silver/transform_clicks.py

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
		--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
		--conf spark.pyspark.python=python3 \
		phase2-spark/src/jobs/explore_clicks.py
