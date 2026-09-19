# sample_data/

Nothing is committed here. This directory exists so `scripts/`, tests, or
benchmarks that write local sample files (e.g. a CSV export used for a
Parquet-vs-CSV benchmark) have a conventional, git-ignored place to put
them (`sample_data/generated/`, ignored — see `.gitignore`).

Reproducible synthetic *database* rows (not files) are seeded directly into
Postgres by `scripts/seed_sample_data.py` — see that file and
`docs/analytics-engineering-guide.md`.
