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
"Testing", Category I interview question for the reasoning spelled out in
full.
