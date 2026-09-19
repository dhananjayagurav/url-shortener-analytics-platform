# Source schema: `urls`

Real schema, confirmed by reading the url-shortener application's own
`app/models/url.py` and `migrations/versions/*.py` directly on 2026-09-19 —
not invented. Mirrored locally by `sql/source/001_urls_real_schema.sql`.

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `id` | `BIGINT` (identity) | No | Surrogate PK, autoincrement. |
| `short_code` | `VARCHAR(16)` | **Yes** | Unique, indexed. Nullable because of an id-first generation strategy (migration `957b3fbefa19`) — a row can briefly exist before its code is assigned. |
| `original_url` | `TEXT` | No | The destination URL. |
| `created_at` | `TIMESTAMPTZ` | No | Set at insert. |
| `expires_at` | `TIMESTAMPTZ` | Yes | Present in the schema (migration `cd8736f28600`); **not enforced anywhere in the application code** as of this reading — no scheduled job or query filter acts on it yet. |
| `user_id` | `BIGINT` | Yes | Present in the schema; **there is no `users` table in the real application** (see Section 1.5 of the guide). This column is unenforced — no foreign key constraint. |
| `is_active` | `BOOLEAN` | No | Default `true`. |
| `deleted_at` | `TIMESTAMPTZ` | Yes | Soft-delete marker (migration `ff8ab26cc6ea`). |

## What's missing, relative to what an analytics platform would want

- No `clicks` (or equivalent event) table anywhere. The redirect handler
  (`app/api/urls.py`, `GET /{short_code}`) resolves and returns a 302
  without recording anything.
- No `users` table, despite `urls.user_id` existing as a column.
- `docs/01-requirements.md` in the source repository lists "basic
  analytics: click count, timestamp, coarse geo/device info per short code"
  as an explicit v1 functional requirement — not yet built.

See `docs/analytics-engineering-guide.md`, Section 1.5, for the full,
honest write-up of this gap and how this platform's `users`/`clicks` tables
(clearly labeled hypothetical) stand in for it.
