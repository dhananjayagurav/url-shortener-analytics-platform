# sql/analytics/

DDL for the star schema (`dim_date`, `dim_url`, `dim_user`, `dim_device`,
`fact_clicks`). Design narrative: `docs/analytics-engineering-guide.md`,
Sections 9-11. Column-level reference:
[`schemas/analytics/dimensional-model.md`](../../schemas/analytics/dimensional-model.md).

**Not auto-applied by `make up`** — unlike `sql/source/`, which is mounted
into Postgres's `docker-entrypoint-initdb.d` and runs on first container
start, these files are applied explicitly:

```bash
make up                       # Postgres + MinIO, as usual
make create-analytics-schema  # applies sql/analytics/*.sql, in order
```

This split is deliberate: `sql/source/` defines the OLTP mirror the
ingestion pipeline reads from and needs to exist before anything else can
run; `sql/analytics/` defines a target schema that nothing writes to yet
(Phase 2's transform does that) — auto-creating it on every fresh
`make up` would be harmless but would blur a distinction worth keeping
visible while it's still schema-only. See the guide's Section 11 for the
full reasoning.

`dim_date` and `dim_user` self-populate at DDL time (a generated calendar
spine and a single Unknown-member row, respectively) — everything else is
schema only until Phase 2 writes to it.
