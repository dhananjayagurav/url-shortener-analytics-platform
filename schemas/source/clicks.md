# Source schema: `clicks`

**HYPOTHETICAL** — see `schemas/source/urls.md` and
`docs/analytics-engineering-guide.md` Section 1.5 / ADR-008 for why. Does
not exist in the real `url-shortener` application. Defined locally by
`sql/source/002_hypothetical_users_and_clicks.sql`, formalized as a data
contract at `contracts/source/clicks.yaml` (Section 12). This is the table
Section 15's incremental-load watermark reads from, and the table Section
7's metrics catalog is built around — it's the one true event/fact source
in this project's OLTP mirror.

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `id` | `BIGSERIAL` | No | Surrogate PK, autoincrement, strictly insertion-ordered. This is the column Section 15's id-based watermark reads (`WHERE id > :watermark`). |
| `short_code` | `VARCHAR(16)` | No | References `urls.short_code` informally — no enforced FK (matches the real application's own pattern of leaving `urls.user_id` unenforced; see `urls.md`). A `short_code` that no longer exists in `urls` (e.g. hard-deleted) would still have historical `clicks` rows — see the "orphaned foreign key" scenario in Section 12. |
| `occurred_at` | `TIMESTAMPTZ` | No | `DEFAULT now()`. **Not** what this project's watermark uses (see ADR-005's rejection of timestamp-based watermarking) — used only as a business-time attribute for analytical grain (`dim_date`, Section 10). |
| `device_type` | `VARCHAR(16)` | No | `mobile` \| `desktop` \| `tablet` \| `unknown`. Default `unknown`. Seeded data only ever writes `mobile`/`desktop`/`tablet` (weighted 60/35/5) — `unknown` exists in the contract for real-world clients that can't be classified, and is exercised by `dim_device`'s own Unknown-member convention (Section 10.3). |
| `hashed_ip` | `VARCHAR(64)` | Yes | SHA-256 hex digest — **never a raw IP**, by construction (see the column comment in `sql/source/002_hypothetical_users_and_clicks.sql` and Section 23, PII, planned). Not currently used by any metric in Section 7's catalog; kept because a real "basic analytics" requirement (coarse geo/device info) would eventually need *some* client-identifying signal, and hashing it at the source is the safer default to model from day one rather than retrofit later. |
| `user_id` | `BIGINT` | Yes | `NULL` for anonymous clicks — ~70% of rows in the seeded data. No enforced FK to `users.id`. This is the column `dim_user`'s Unknown-member convention exists to handle cleanly in the fact table (Section 10.3). |

## Grain

One row per redirect (click) event — the finest grain this table can
produce, and (per Section 8's Design Decision) the grain `fact_clicks`
keeps, unaggregated, in the analytical model too.

## Known quirks worth knowing before extracting from this table

- **Insert-only, by data contract** (Section 12): nothing in this schema
  enforces immutability at the database level (no trigger preventing
  `UPDATE`/`DELETE`), but Section 15's entire watermark mechanism —
  and, downstream, `fact_clicks`' load strategy — depends on this table
  only ever growing via `INSERT`. This is exactly the assumption named
  explicitly in Section 15.9's interview question about what an id-based
  watermark structurally cannot capture.
- `occurred_at` is business time (when the redirect happened); the
  Bronze `ingestion_date=`/`watermark_start=...watermark_end=` partitioning
  in `object_store.py` is *processing* time (when this pipeline extracted
  the row). The two are related but not identical — a `clicks` row can be
  extracted on a different calendar day than the one it occurred on if a
  run is delayed. Section 10's `dim_date` join uses `occurred_at`, not
  extraction time, precisely because analytical questions ("clicks per
  day") care about business time.
