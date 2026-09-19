# ingestion/

The Phase 1 batch ingestion package: `url_shortener_analytics`.

```
ingestion/
├── src/url_shortener_analytics/   importable package (pip install -e ".[dev]")
│   ├── config.py                  Settings (env vars / .env)
│   ├── logging_setup.py           structured logging
│   ├── db.py                      OLTP SQLAlchemy Engine
│   ├── object_store.py            Bronze writes to MinIO/S3 (Parquet)
│   ├── metadata.py                watermark / checkpoint / run history
│   ├── extract_full.py            full-load extraction + orchestration
│   └── cli.py                     `python -m url_shortener_analytics.cli full-load`
├── tests/
│   ├── unit/                      SQLite + mocked S3 -- no Docker required
│   └── integration/                real Postgres + MinIO -- `make test-integration`
└── configs/
    └── pipelines.yaml             which tables, which load type
```

See `docs/analytics-engineering-guide.md` at the repository root for the
full teaching material behind every module above -- concept, why it exists,
how to run it, hands-on labs, failure scenarios, and interview questions.
This file is a map, not a tutorial.
