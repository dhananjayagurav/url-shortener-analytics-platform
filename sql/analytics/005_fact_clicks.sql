-- Transaction fact table, grain = one row per click event (Section 8).
-- See docs/analytics-engineering-guide.md, Section 10 ("fact_clicks"), and
-- schemas/analytics/dimensional-model.md for the full column reference,
-- and Section 11 for the star-vs-normalized design decision this table's
-- shape is the outcome of.
--
-- Not yet populated -- Phase 2's transform reads Bronze
-- (bronze/clicks/... Parquet, both full-load and incremental objects) and
-- writes here. Schema committed now so that transform has a stable,
-- reviewed target.

CREATE TABLE IF NOT EXISTS fact_clicks (
    click_id        BIGINT PRIMARY KEY,               -- natural key, source clicks.id
    date_key        INTEGER NOT NULL REFERENCES dim_date (date_key),
    url_key         INTEGER NOT NULL REFERENCES dim_url (url_key),
    user_key        INTEGER NOT NULL REFERENCES dim_user (user_key),   -- never NULL; -1 = Unknown/anonymous
    device_key      INTEGER NOT NULL REFERENCES dim_device (device_key),
    short_code      VARCHAR(16) NOT NULL,              -- degenerate dimension
    occurred_at     TIMESTAMPTZ NOT NULL,               -- degenerate attribute, business time
    click_count     SMALLINT NOT NULL DEFAULT 1,        -- additive measure, always 1 at this grain
    row_loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE fact_clicks IS
    'Transaction fact, one row per click. user_key is NEVER NULL -- '
    'anonymous clicks resolve to dim_user''s Unknown member (user_key=-1), '
    'not a NULL foreign key. See Section 10.3 for why that distinction '
    'matters for every downstream BI tool''s default join behavior.';

CREATE INDEX IF NOT EXISTS ix_fact_clicks_date_key   ON fact_clicks (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_clicks_url_key    ON fact_clicks (url_key);
CREATE INDEX IF NOT EXISTS ix_fact_clicks_user_key   ON fact_clicks (user_key);
CREATE INDEX IF NOT EXISTS ix_fact_clicks_device_key ON fact_clicks (device_key);
