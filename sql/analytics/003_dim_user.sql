-- SCD Type 1 dimension for users, deliberately excluding email (PII --
-- see docs/analytics-engineering-guide.md, Section 10.4). Includes a
-- designated Unknown member row (user_key = -1) so fact_clicks.user_key
-- never has to be NULL for an anonymous click -- see Section 10.3 for why
-- a NULL foreign key in a fact table is worth actively designing around.

CREATE TABLE IF NOT EXISTS dim_user (
    user_key       SERIAL PRIMARY KEY,     -- surrogate key; -1 reserved for the Unknown member
    user_id        BIGINT NOT NULL UNIQUE, -- natural key, source users.id; -1 for the Unknown member
    plan_type      VARCHAR(16) NOT NULL,
    is_known       BOOLEAN NOT NULL,
    row_loaded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE dim_user IS
    'SCD Type 1. Excludes email by design -- the analytical layer only '
    'ever needs plan_type (see the Section 7 metrics catalog). Row '
    'user_key=-1 is the designated Unknown member for anonymous clicks.';

-- The Unknown member -- inserted here, at DDL time, exactly like dim_date's
-- calendar spine, NOT by the (not-yet-built) Phase 2 transform. A fact
-- table that references dim_user before Phase 2 runs must still be able to
-- resolve user_key=-1 to a real row. Explicitly setting user_key=-1 does
-- not disturb the SERIAL sequence -- it still hands out 1, 2, 3, ... to
-- every row Phase 2's transform inserts without specifying user_key.
INSERT INTO dim_user (user_key, user_id, plan_type, is_known)
VALUES (-1, -1, 'UNKNOWN', false)
ON CONFLICT (user_key) DO NOTHING;
