from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

class Phase2Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Object store
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "labadmin"
    minio_secret_key: str = "labpassword"
    minio_bucket: str = "analytics-lake"

    # Spark cluster
    spark_master_url: str = "spark://localhost:7077"
    spark_app_name: str = "phase2-spark"


@lru_cache
def get_phase2_settings() -> Phase2Settings:
    return Phase2Settings()

