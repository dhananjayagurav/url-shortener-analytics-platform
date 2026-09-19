-- Conformed date dimension. Not derived from any source table -- generated
-- once, here, for a fixed calendar range. See
-- docs/analytics-engineering-guide.md, Section 10 ("dim_date"), and
-- schemas/analytics/dimensional-model.md.
--
-- Not auto-applied by `make up` (unlike sql/source/, which IS mounted into
-- the Postgres init directory) -- this is analytical-layer schema, applied
-- explicitly with `make create-analytics-schema`. See the guide's Section
-- 11 for why that split is deliberate.

CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INTEGER PRIMARY KEY,      -- YYYYMMDD, e.g. 20260919
    full_date    DATE NOT NULL UNIQUE,
    day_of_week  SMALLINT NOT NULL,        -- 0=Sunday .. 6=Saturday (matches Postgres EXTRACT(DOW))
    day_name     VARCHAR(9) NOT NULL,
    day_of_month SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    month_name   VARCHAR(9) NOT NULL,
    quarter      SMALLINT NOT NULL,
    year         SMALLINT NOT NULL,
    is_weekend   BOOLEAN NOT NULL
);

COMMENT ON TABLE dim_date IS
    'Conformed date dimension. date_key = YYYYMMDD as an INTEGER -- sorts '
    'and joins cheaply, human-readable without a join. Populated once, '
    'below, not by any ETL run.';

INSERT INTO dim_date (
    date_key, full_date, day_of_week, day_name, day_of_month,
    month, month_name, quarter, year, is_weekend
)
SELECT
    CAST(to_char(d, 'YYYYMMDD') AS INTEGER),
    d,
    EXTRACT(DOW FROM d)::SMALLINT,
    to_char(d, 'Day'),
    EXTRACT(DAY FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    to_char(d, 'Month'),
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;
