-- SCD Type 1 dimension for urls. See docs/analytics-engineering-guide.md,
-- Section 10 ("dim_url"), for why Type 1 (not Type 2) is the deliberate
-- Phase 1 choice, and schemas/analytics/dimensional-model.md for the full
-- column reference.

CREATE TABLE IF NOT EXISTS dim_url (
    url_key              SERIAL PRIMARY KEY,        -- surrogate key
    url_id               BIGINT NOT NULL UNIQUE,     -- natural key, source urls.id
    short_code           VARCHAR(16),
    original_url_domain  VARCHAR(255),               -- derived: hostname of original_url
    is_active            BOOLEAN NOT NULL,
    created_date_key     INTEGER REFERENCES dim_date (date_key),
    row_loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE dim_url IS
    'SCD Type 1 (overwrite-on-change). url_id is the natural key from the '
    'source urls table; url_key is the surrogate key fact_clicks joins on. '
    'Not yet populated -- Phase 2''s transform writes this table.';

CREATE INDEX IF NOT EXISTS ix_dim_url_short_code ON dim_url (short_code);
