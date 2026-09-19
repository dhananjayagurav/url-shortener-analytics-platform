# Source schema: `users`

**HYPOTHETICAL** — see `schemas/source/urls.md` and
`docs/analytics-engineering-guide.md` Section 1.5 / ADR-008 for why. Does
not exist in the real `url-shortener` application. Defined locally by
`sql/source/002_hypothetical_users_and_clicks.sql`, formalized as a data
contract at `contracts/source/users.yaml` (Section 12).

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `id` | `BIGSERIAL` | No | Surrogate PK, autoincrement. This is the value `urls.user_id` and `clicks.user_id` reference informally (no enforced FK from `urls`, since `user_id` predates this table in the real schema — see `urls.md`). |
| `email` | `VARCHAR(255)` | No | Unique. Never leaves Bronze in raw form in the analytical layer — see `dim_user`'s design decision in `schemas/analytics/dimensional-model.md` and Section 23 (PII, planned). |
| `plan_type` | `VARCHAR(16)` | No | `FREE` \| `PREMIUM`. Default `FREE`. The one attribute Section 7's metrics actually need from this table. |
| `created_at` | `TIMESTAMPTZ` | No | Set at insert, `DEFAULT now()`. |

## Grain

One row per registered user. `id` is unique and stable for the row's
lifetime — this table has no soft-delete or update path modeled yet (POC
SIMPLIFICATION; a real accounts table would need at least an update
timestamp — see the Source Data Model Design Decision in Section 9 for why
that's deliberately out of scope here).

## Known quirks worth knowing before extracting from this table

- `plan_type` is a free-text `VARCHAR`, not a Postgres `ENUM` or a foreign
  key to a `plans` lookup table — nothing at the database level prevents a
  third value. `scripts/seed_sample_data.py` only ever writes `FREE` or
  `PREMIUM` (weighted 85/15), but the analytics layer should not assume
  the column is closed to exactly those two values — see the "extra value
  in an enum-like column" scenario in Section 12's Failure Scenario.
- ~70% of `clicks.user_id` values are `NULL` (anonymous clicks) in the
  seeded data — `users` itself has no concept of an "anonymous" member;
  that's handled entirely on the analytical side, in `dim_user`'s
  designated Unknown-member row (Section 10.3).
