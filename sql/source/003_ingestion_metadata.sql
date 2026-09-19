-- Run-level bookkeeping for the ingestion pipeline: watermark, checkpoint,
-- and run metadata combined into one audit table. See
-- docs/analytics-engineering-guide.md, "Checkpoints and Watermarks", and
-- ingestion/src/url_shortener_analytics/metadata.py for the code that reads
-- and writes this table.

CREATE TABLE IF NOT EXISTS ingestion_metadata (
    run_id          UUID PRIMARY KEY,
    pipeline_name   VARCHAR(64)  NOT NULL,
    source_table    VARCHAR(64)  NOT NULL,
    load_type       VARCHAR(16)  NOT NULL,   -- full | incremental
    status          VARCHAR(16)  NOT NULL,   -- running | success | failed
    watermark_start BIGINT,
    watermark_end   BIGINT,
    rows_read       BIGINT,
    rows_written    BIGINT,
    bronze_key      VARCHAR(512),            -- the exact object written on success; NULL on failure or a no-op run
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    error_message   TEXT
);

-- Idempotent-safe for an existing dev database too, not just a fresh
-- volume -- docker-entrypoint-initdb.d scripts only ever run once, on
-- first container init, so an already-running local Postgres needs this
-- to pick up the column without a full `make down -v && make up`. See
-- docs/analytics-engineering-guide.md, Section 17, for why bronze_key was
-- added.
ALTER TABLE ingestion_metadata ADD COLUMN IF NOT EXISTS bronze_key VARCHAR(512);

CREATE INDEX IF NOT EXISTS ix_ingestion_metadata_lookup
    ON ingestion_metadata (pipeline_name, source_table, status, started_at DESC);
