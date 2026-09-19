# benchmarks/

`parquet_vs_csv_vs_json.py` — Parquet vs. CSV vs. JSON-lines: file size,
write time, full-read time, single-column-read time, run against this
project's own real `clicks` table or a larger synthetic dataset of the
same shape. See docs/analytics-engineering-guide.md, Section 19 (Parquet),
for the concept and the real, genuinely-run results (also Section 20,
Partitioning, for the related `list_bronze_keys_for_date_range` pruning
work, and Section 26 for future extraction-time-at-scale benchmarks not
yet built).

Run: `make benchmark-parquet` (real data) or
`make benchmark-parquet ROWS=200000` (synthetic, larger scale).

Per the guide's rule on fabricated results: nothing in this directory's
`results/` is committed (see `.gitignore`) and no number is written into
`docs/analytics-engineering-guide.md` without the script that produced it
having actually been run first.
