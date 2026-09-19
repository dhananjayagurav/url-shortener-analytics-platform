"""Configuration, loaded from environment variables / a .env file.

Mirrors the pattern already used by the url-shortener application itself
(app/config.py: pydantic-settings BaseSettings + a cached getter) so an
engineer moving between the two repositories sees one consistent way of
doing configuration, not two.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Postgres (this platform's own OLTP-mirror instance -- ADR-009) ---
    database_url: str = "postgresql+psycopg://analytics:analytics@localhost:5433/analytics"

    # --- MinIO / S3-compatible object storage ---
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "labadmin"
    minio_secret_key: str = "labpassword"
    minio_bucket: str = "analytics-lake"

    # --- Ingestion behavior ---
    log_level: str = "INFO"
    pipeline_name: str = "url_shortener_bronze_ingestion"


@lru_cache
def get_settings() -> Settings:
    """Cached settings singleton.

    lru_cache (not a module-level global) so tests can call
    `get_settings.cache_clear()` between cases that set different
    environment variables, without import-order surprises.
    """
    return Settings()
