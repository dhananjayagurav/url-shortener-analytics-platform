# contracts/

Formal data contracts — machine-checkable, versioned agreements about a
table's shape, ownership, and freshness — validated against the real
database by
[`ingestion/src/url_shortener_analytics/contracts.py`](../ingestion/src/url_shortener_analytics/contracts.py).
See `docs/analytics-engineering-guide.md`, Section 12, for the concept,
design decision (why coarse type categories, why violations vs. warnings),
and how this differs from `schemas/source/*.md` (human-readable narrative
documentation — not machine-checked) and `schemas/analytics/dimensional-model.md`
(the analytical-layer design, not yet contract-checked — see Section 12's
Production Considerations for why that's a deliberate Phase 1 scope limit).

Run: `make validate-contracts` (requires `make up` — validates against the
real Postgres mirror).
