# transformations/

The Phase 2 transformation package: `analytics_transform`. Reads Bronze,
writes Silver (and, in a later milestone, Gold) using Apache Spark.

```
transformations/
├── src/analytics_transform/       importable package (pip install -e ".[dev,transform]")
│   ├── config.py                  TransformSettings -- local Bronze/Silver/Gold data lake root
│   └── silver/
│       └── transform_clicks.py    Bronze clicks -> Silver clicks (Phase 2's first component)
└── tests/
    └── unit/                      real local[1] SparkSession, deliberately dirty rows -- no Docker required
```

Only what the first milestone needs exists so far -- no `bronze/`, `gold/`,
`quality/`, `profiling/`, `deduplication/`, `dimensions/`, or `facts/`
subpackage yet, on purpose (see `docs/analytics-engineering-guide.md`,
Architectural Principles, "don't build it until it's earned" -- the same
principle Section 35 already applied to the two near-identical benchmark
`cleanup()` functions). Each later Phase 2 milestone adds exactly the
subpackage its own component needs, when that component's code exists to
put there.

**No real MinIO/S3 in this environment** (see the guide's Phase 2,
ADR-016): `analytics_transform.config`'s `bronze_root`/`silver_root` point
at a local `data/` directory (gitignored) instead of a bucket.
`scripts/write_local_bronze_clicks.py` (at the repository root, not part
of this package) produces a real Bronze `clicks` Parquet snapshot there,
using the real `extract_full` function against real Postgres -- see that
script's own docstring for exactly what is and isn't substituted, and why.

See `docs/analytics-engineering-guide.md` at the repository root for the
full teaching material behind every module above -- concept, why it
exists, architecture, design decision, how to run it, how to verify it,
the hands-on exercise, the failure scenario, and interview questions. This
file is a map, not a tutorial.
