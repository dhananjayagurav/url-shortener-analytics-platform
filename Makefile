.PHONY: venv install up down logs ps db-shell ingest ingest-full create-analytics-schema validate-contracts check-stale-runs reconcile-bronze storage-stats benchmark-parquet benchmark-query-performance benchmark-extraction-time layout-report ingestion-history ingestion-summary pii-report test test-integration coverage lint fmt seed

venv:
	python3.12 -m venv .venv
	@echo "Activate with: source .venv/bin/activate"

install:
	pip install -e ".[dev]"

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

db-shell:
	docker compose exec postgres psql -U $${POSTGRES_USER:-urlshortener} -d $${POSTGRES_DB:-urlshortener}

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
		docker compose exec -T postgres psql -U $${POSTGRES_USER:-urlshortener} -d $${POSTGRES_DB:-urlshortener} -f - < $$f; \
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

# See docs/analytics-engineering-guide.md Section 24. Measured, not
# gated -- no --cov-fail-under threshold yet (Section 24.3).
coverage:
	pytest --cov=url_shortener_analytics --cov-report=term-missing

lint:
	ruff check .

fmt:
	ruff format .
