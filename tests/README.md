# tests/ (repository root)

Reserved for cross-cutting, end-to-end tests that exercise more than one
phase's components together (e.g., once Phase 2 adds Silver transformation,
a test here might prove Bronze → Silver → Gold end to end).

This is deliberately distinct from `ingestion/tests/`, which holds unit and
integration tests scoped to the ingestion package alone — see that
directory and docs/analytics-engineering-guide.md, "Project Structure", for
the reasoning. Empty for now: Phase 1 has only one component, so there is
nothing cross-cutting yet to test here.
