"""OLTP database access.

One function, one job: build a SQLAlchemy Engine from configuration. An
Engine already manages its own connection pool and is safe to share across
calls within a process, so there is deliberately no session/connection
wrapper here yet -- extract_full.py takes an Engine directly. A thin
`get_session()` context manager is a natural place to grow this file when
Phase 2 needs transactional multi-statement writes.
"""

from __future__ import annotations

from functools import lru_cache

from sqlalchemy import Engine, create_engine

from url_shortener_analytics.config import Settings


@lru_cache
def get_engine(database_url: str) -> Engine:
    """Return a cached Engine for a given URL.

    Keyed by the URL string (not by a Settings object, which isn't
    hashable in a stable way) so tests can point at a throwaway SQLite
    database without needing a real Postgres.
    """
    return create_engine(database_url, pool_pre_ping=True)


def engine_from_settings(settings: Settings) -> Engine:
    return get_engine(settings.database_url)
