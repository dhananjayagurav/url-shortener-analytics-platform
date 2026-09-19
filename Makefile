.PHONY: venv install up down logs ps db-shell ingest ingest-full test test-integration lint fmt seed

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

test:
	pytest -v

test-integration:
	pytest -v -m integration

lint:
	ruff check .

fmt:
	ruff format .
