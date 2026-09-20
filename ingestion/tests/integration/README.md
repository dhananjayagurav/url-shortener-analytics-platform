# Integration tests

These tests exercise the real Postgres + MinIO containers this repo's
`docker-compose.yml` brings up. They are marked `@pytest.mark.integration`
and excluded by default (`pyproject.toml`'s `addopts = "-m 'not integration'"`),
so `make test` / plain `pytest` never silently fails just because Docker
isn't running.

## Run them

```bash
docker compose up -d
# wait for `docker compose ps` to show postgres and minio as healthy
make test-integration
```

## Why these are separate from ingestion/tests/unit/

Unit tests (`ingestion/tests/unit/`) run against an in-memory SQLite engine
and a mocked S3 client — they test this package's own logic (watermark
transitions, retry behavior, checkpoint state machine) in isolation, in
under a second, with no external dependency. They intentionally do **not**
prove the code works against real Postgres/MinIO wire behavior (SQL dialect
differences, real network errors, real S3 API semantics).

Integration tests close that gap: they prove the same code paths work
end-to-end against the real thing. Both layers exist because they catch
different classes of bug — see docs/analytics-engineering-guide.md,
Section 24 ("Testing"), Category I interview question, for the reasoning
spelled out in full.

## Running these without Docker

`test_contracts_integration.py` only needs a real Postgres, not MinIO --
if one is reachable some other way (as in the sandbox this project's
guide was written in, which installed Postgres 16 directly, listening on
the standard port 5432 rather than docker-compose's 5433 mapping), it
can run without Docker at all:

```bash
DATABASE_URL="postgresql+psycopg://analytics:analytics@localhost:5432/analytics" \
  pytest ingestion/tests/integration/test_contracts_integration.py -v -m integration
```

Without that override, `Settings()`'s class-level default (port 5433)
is used, and the test fails with a real, genuine `OperationalError`
naming the wrong port -- not a bug in the code under test. See Section
24.7's Failure Scenario, and LAB 20, for this exact failure reproduced
and explained. `test_full_load_integration.py` and
`test_incremental_load_integration.py` still need real MinIO regardless
-- there is no equivalent workaround for those two.
