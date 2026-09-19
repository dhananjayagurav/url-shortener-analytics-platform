-- Mirrors the REAL url-shortener application's `urls` table, as confirmed by
-- reading its actual SQLAlchemy model (app/models/url.py) and Alembic
-- migration history (migrations/versions/*.py) on 2026-09-19. This file is a
-- MIRROR for this platform's own standalone Postgres (see ADR-009) -- the
-- url-shortener repo's own migrations remain the real source of truth, and
-- this file must be re-synced by hand if that schema changes.
--
-- Migration history this reflects, in order:
--   3813da2d7b76  baseline: id, short_code, original_url, created_at
--   cd8736f28600  + expires_at, user_id, is_active
--   ff8ab26cc6ea  + deleted_at
--   957b3fbefa19  short_code made nullable (id-first generation support)

CREATE TABLE IF NOT EXISTS urls (
    id           BIGSERIAL PRIMARY KEY,
    short_code   VARCHAR(16) UNIQUE,                 -- nullable: id-first generation writes this after insert
    original_url TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ,                        -- present in the schema; not yet enforced anywhere in app code
    user_id      BIGINT,                              -- present in the schema; NO users table exists in the real app (see guide Section 1.5)
    is_active    BOOLEAN NOT NULL DEFAULT true,
    deleted_at   TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS ix_urls_short_code ON urls (short_code);
