-- HYPOTHETICAL TABLES. The real url-shortener application has neither a
-- `users` table nor a `clicks` table as of 2026-09-19 (confirmed by reading
-- the app's models/ and api/ directories directly -- the redirect handler in
-- app/api/urls.py resolves and returns a 302 without recording anything).
--
-- `urls.user_id` exists as a bare, unenforced BIGINT column, and
-- docs/01-requirements.md lists "basic analytics: click count, timestamp,
-- coarse geo/device info per short code" as an explicit v1 functional
-- requirement (#5) that is not yet built. These two tables give this
-- analytics platform something real to extract from, standing in for that
-- not-yet-built capability, clearly labeled as hypothetical everywhere they
-- are referenced. See docs/analytics-engineering-guide.md, Section 1.5 and
-- ADR-008 for the full reasoning.

COMMENT ON TABLE urls IS 'Real schema, mirrored from the url-shortener application.';

CREATE TABLE IF NOT EXISTS users (
    id         BIGSERIAL PRIMARY KEY,
    email      VARCHAR(255) UNIQUE NOT NULL,
    plan_type  VARCHAR(16) NOT NULL DEFAULT 'FREE',    -- FREE | PREMIUM
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE users IS
    'HYPOTHETICAL. Does not exist in the real url-shortener application. '
    'Built here so ingestion has a users source to extract, matching the '
    'urls.user_id column that already exists but is unenforced upstream.';

CREATE TABLE IF NOT EXISTS clicks (
    id          BIGSERIAL PRIMARY KEY,
    short_code  VARCHAR(16) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    device_type VARCHAR(16) NOT NULL DEFAULT 'unknown', -- mobile | desktop | tablet | unknown
    hashed_ip   VARCHAR(64),                             -- SHA-256 hex digest, never a raw IP -- see Section 20 (PII)
    user_id     BIGINT                                   -- NULL for anonymous clicks
);

COMMENT ON TABLE clicks IS
    'HYPOTHETICAL. Does not exist in the real url-shortener application -- '
    'the redirect handler currently records nothing. Stands in for '
    'docs/01-requirements.md functional requirement #5 ("basic analytics"), '
    'not yet implemented upstream.';

CREATE INDEX IF NOT EXISTS ix_clicks_short_code  ON clicks (short_code);
CREATE INDEX IF NOT EXISTS ix_clicks_occurred_at ON clicks (occurred_at);
