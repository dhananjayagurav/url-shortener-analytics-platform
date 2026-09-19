# schemas/analytics/

The dimensional model (fact/dimension tables, star schema): design,
grain, SCD decisions, and conformance — see
[`dimensional-model.md`](dimensional-model.md). DDL that implements it
lives in [`sql/analytics/`](../../sql/analytics/); full design narrative
(requirements, grain, alternatives, trade-offs) is in
`docs/analytics-engineering-guide.md`, Sections 9-11.

Schema only, for now — no transform populates these tables yet. That's
Phase 2's job ("Data Lake, Transformation & Data Quality"); see the
guide's Section 36, "What Phase 2 Will Add".
