# benchmarks/

Executable benchmark scripts (Parquet vs. CSV/JSON, partitioned vs.
unpartitioned reads, extraction time at increasing row counts) land here in
a later Phase 1 increment — see docs/analytics-engineering-guide.md,
Sections 22-23 (Parquet, Partitioning) and 29 (Performance).

Per the guide's rule on fabricated results: nothing in this directory's
`results/` is committed (see `.gitignore`) and no number is written into
`docs/analytics-engineering-guide.md` without the script that produced it
having actually been run first.
