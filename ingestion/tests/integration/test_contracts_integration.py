"""Integration test: validate the real contracts/source/*.yaml files
against the real Postgres this repo's docker-compose.yml brings up.
Requires:

    docker compose up -d
    make test-integration

See docs/analytics-engineering-guide.md, Section 12, and
ingestion/tests/integration/README.md.

NOT YET EXECUTED in this sandbox -- no Docker daemon available here. The
unit test `test_validate_all_contracts_against_the_real_contracts_directory`
(SQLite) already exercises the same contract files against a hand-built
matching schema; this test is the stronger proof, against the actual
`sql/source/*.sql`-created tables.
"""

from __future__ import annotations

import pytest

from url_shortener_analytics.config import Settings
from url_shortener_analytics.contracts import validate_all_contracts
from url_shortener_analytics.db import engine_from_settings


@pytest.fixture
def settings() -> Settings:
    return Settings()


@pytest.mark.integration
def test_all_source_contracts_pass_against_the_real_schema(settings: Settings) -> None:
    """If this fails, either sql/source/*.sql drifted from
    contracts/source/*.yaml, or vice versa -- one of the two needs to be
    updated to match the other. See Section 12's Failure Scenario for
    which direction that fix should usually go in."""
    engine = engine_from_settings(settings)

    results = validate_all_contracts(engine)

    failed = [r for r in results if not r.passed]
    assert failed == [], f"contract(s) failed against real Postgres: {failed}"
