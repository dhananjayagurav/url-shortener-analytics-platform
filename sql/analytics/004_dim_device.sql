-- Small, static-ish dimension for device_type. See
-- docs/analytics-engineering-guide.md, Section 10 ("dim_device"), and
-- schemas/analytics/dimensional-model.md for why "unknown" here is real
-- source data (contracts/source/clicks.yaml's own default), not a
-- synthesized NULL-avoidance row like dim_user's.

CREATE TABLE IF NOT EXISTS dim_device (
    device_key   SERIAL PRIMARY KEY,
    device_type  VARCHAR(16) NOT NULL UNIQUE
);

COMMENT ON TABLE dim_device IS
    'One row per distinct clicks.device_type value. Seeded with the '
    'values contracts/source/clicks.yaml declares as valid -- kept in '
    'sync with the contract by hand for now (Phase 1 scale); see Section '
    '12''s Production Considerations for what changes at real scale.';

INSERT INTO dim_device (device_type)
VALUES ('mobile'), ('desktop'), ('tablet'), ('unknown')
ON CONFLICT (device_type) DO NOTHING;
