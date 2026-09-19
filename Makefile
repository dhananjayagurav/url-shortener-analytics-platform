.PHONY: venv install up down logs ps db-shell ingest ingest-full create-analytics-schema validate-contracts check-stale-runs reconcile-bronze test test-integration lint fmt seed

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

test:
	pytest -v

test-integration:
	pytest -v -m integration

lint:
	ruff check .

fmt:
	ruff format .
