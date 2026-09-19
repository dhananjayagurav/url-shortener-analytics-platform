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
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS ix_ingestion_metadata_lookup
    ON ingestion_metadata (pipeline_name, source_table, status, started_at DESC);
