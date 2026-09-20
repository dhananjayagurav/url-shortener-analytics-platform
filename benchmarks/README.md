# benchmarks/

`parquet_vs_csv_vs_json.py` — Parquet vs. CSV vs. JSON-lines: file size,
write time, full-read time, single-column-read time, run against this
project's own real `clicks` table or a larger synthetic dataset of the
same shape. See docs/analytics-engineering-guide.md, Section 19 (Parquet),
for the concept and the real, genuinely-run results (also Section 20,
Partitioning, for the related `list_bronze_keys_for_date_range` pruning
work).

Run: `make benchmark-parquet` (real data) or
`make benchmark-parquet ROWS=200000` (synthetic, larger scale).

`query_performance.py` — runs every metric in Section 7.1's metrics
catalog directly against `fact_clicks`, at several synthetic row-count
scales, and times each one. `fact_clicks` is schema-only as of Section 26
(Phase 2's transform doesn't exist yet), so this script temporarily
populates `fact_clicks`/`dim_url`/`dim_user` with clearly-marked SYNTHETIC
rows (`click_id`/`url_key`/`user_key` above a fixed floor, well clear of
any pre-existing row), times the real queries, then deletes every row it
inserted. See Section 26 for the concept and the real, genuinely-run
numbers this produced, and how they answer Section 7.1's own question
about pre-aggregation. Needs only Postgres, not MinIO.

Run: `make benchmark-query-performance` (default scales: 5,000 / 50,000 /
500,000) or `make benchmark-query-performance SCALES=10000,100000`.

`extraction_time.py` — times this project's own `extract_full` and
`extract_incremental` functions against the real source `clicks` table,
at several scales. Temporarily inserts SYNTHETIC rows (`id` above a fixed
floor, well clear of the real table's contiguous ids) into `clicks`, times
both extraction functions, then deletes every row it inserted. See Section
26 for the concept and the real, genuinely-run numbers. Needs only
Postgres, not MinIO -- this measures extraction only, not the Bronze-write
stage, which does need MinIO (an explicitly named open gap, Section 26.8).

Run: `make benchmark-extraction-time` (default scales: 5,000 / 50,000 /
500,000) or `make benchmark-extraction-time SCALES=10000,100000`.

Per the guide's rule on fabricated results: nothing in this directory's
`results/` is committed (see `.gitignore`) and no number is written into
`docs/analytics-engineering-guide.md` without the script that produced it
having actually been run first.
