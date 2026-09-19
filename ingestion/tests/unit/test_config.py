from __future__ import annotations

import pytest

from url_shortener_analytics.config import Settings, get_settings


def test_settings_have_safe_local_defaults() -> None:
    """Defaults must work against this repo's own docker-compose.yml
    out of the box -- no .env required for a first `make up && make test`."""
    settings = Settings(_env_file=None)  # ignore any real .env on disk for this test
    assert settings.database_url.startswith("postgresql+psycopg://")
    assert settings.minio_bucket == "analytics-lake"
    assert settings.pipeline_name == "url_shortener_bronze_ingestion"


def test_settings_read_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINIO_BUCKET", "custom-bucket")
    get_settings.cache_clear()
    try:
        assert get_settings().minio_bucket == "custom-bucket"
    finally:
        get_settings.cache_clear()
