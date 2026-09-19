"""Integration test: a real full load against the real Postgres + MinIO
this repo's docker-compose.yml brings up. Requires:

    docker compose up -d
    make test-integration

These are NOT run by `make test` / plain `pytest` -- see pyproject.toml's
`addopts = "-m 'not integration'"` and ingestion/tests/integration/README.md
for why, and for the exact commands to run this file specifically.
"""

from __future__ import annotations

import pytest

from url_shortener_analytics.config import Settings
from url_shortener_analytics.db import engine_from_settings
from url_shortener_analytics.extract_full import run_full_load
from url_shortener_analytics.object_store import get_s3_client, head_object


@pytest.fixture
def settings() -> Settings:
    return Settings()


@pytest.mark.integration
def test_full_load_urls_against_real_infrastructure(settings: Settings) -> None:
    engine = engine_from_settings(settings)
    s3_client = get_s3_client(settings)

    result = run_full_load(engine, s3_client, settings.minio_bucket, "integration_test", "urls")

    assert result["rows"] >= 0
    obj = head_object(s3_client, settings.minio_bucket, result["key"])
    assert obj is not None, "the Bronze object run_full_load reported writing should actually exist in MinIO"


@pytest.mark.integration
def test_rerunning_full_load_overwrites_not_duplicates(settings: Settings) -> None:
    """LAB 4/5 as an assertion: same-day reruns must not create duplicate
    Bronze objects. See docs/analytics-engineering-guide.md, "Idempotency"."""
    engine = engine_from_settings(settings)
    s3_client = get_s3_client(settings)

    result_1 = run_full_load(engine, s3_client, settings.minio_bucket, "integration_test", "urls")
    result_2 = run_full_load(engine, s3_client, settings.minio_bucket, "integration_test", "urls")

    assert result_1["key"] == result_2["key"]

    prefix = result_1["key"].rsplit("/", 1)[0] + "/"
    listing = s3_client.list_objects_v2(Bucket=settings.minio_bucket, Prefix=prefix)
    assert listing.get("KeyCount", 0) == 1
