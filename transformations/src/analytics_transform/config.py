"""Configuration for Phase 2 transformations.

Mirrors ingestion/src/url_shortener_analytics/config.py's pattern
(pydantic-settings BaseSettings + a cached getter) -- see that file's
docstring for why a cached singleton, not a bare module-level global.

Kept as its own Settings class, not reused from the ingestion package,
because this package has a genuinely different concern: where the data
LAKE lives (Bronze/Silver/Gold root paths), not how to reach the OLTP
database or an S3 bucket. See docs/analytics-engineering-guide.md, Phase 2,
ADR-016.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


def list_local_bronze_files(bronze_root: Path, table_name: str) -> list[Path]:
    """Every real Bronze Parquet file for `table_name`, full-load and
    incremental objects together, sorted for a deterministic read order.

    The local-filesystem analog of `object_store.py`'s `list_bronze_keys`
    -- needed because ADR-016 means there's no real S3 `list_objects_v2`
    call to make against this environment's Bronze layer. Callers should
    read each returned path *individually* (`spark.read.parquet(str(f))`
    per file, then union the results) rather than pointing Spark at
    `bronze_root / table_name` directly -- see
    docs/analytics-engineering-guide.md, Phase 2, Section 39, for the
    real, observed reason: Spark's Hive-style partition discovery cannot
    reconcile Bronze's two different partitioning schemes (full-load's
    `ingestion_date=`, incremental's `watermark_start=`/`watermark_end=`)
    in one directory-level read, and genuinely raises
    `AssertionError: Conflicting directory structures detected` the
    moment both exist under the same table's prefix at once.
    """
    table_root = bronze_root / table_name
    return sorted(table_root.glob("**/*.parquet"))

# transformations/src/analytics_transform/config.py -> repo root is 3
# parents up (analytics_transform -> src -> transformations -> root).
REPO_ROOT = Path(__file__).resolve().parents[3]


class TransformSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Data lake root ---
    # POC SIMPLIFICATION -- ADR-016: this sandbox has no real MinIO/S3
    # reachable (Docker's daemon cannot start here; a standalone MinIO
    # binary isn't on the network allowlist -- both confirmed before this
    # decision was made, not assumed). Bronze/Silver/Gold all live under
    # one local filesystem root instead of an S3 bucket. Production
    # equivalent: replace `bronze_root`/`silver_root` with
    # s3a://<bucket>/bronze and s3a://<bucket>/silver -- Spark's own S3A
    # connector (hadoop-aws) does the rest, and none of transform_clicks.py's
    # logic changes. See the guide's Phase 2, ADR-016, "Production
    # Considerations".
    data_root: Path = REPO_ROOT / "data"

    log_level: str = "INFO"

    @property
    def bronze_root(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver_root(self) -> Path:
        return self.data_root / "silver"


@lru_cache
def get_transform_settings() -> TransformSettings:
    """Cached settings singleton -- same lru_cache pattern (and the same
    reason: tests calling .cache_clear() between cases) as
    url_shortener_analytics.config.get_settings."""
    return TransformSettings()
